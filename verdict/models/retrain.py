"""The job a drift request starts, and the pull request it opens.

`PLAN.md` section 2.6 and ADR 12: when the monitors report the same quantity
drifting on two consecutive days, a retraining request opens
(`drift/trigger.py`). This is what happens next, and where it stops.

The job fits a candidate on the history that has arrived, exports it, scores
it against the incumbent champion on rows neither was trained on, and writes
a pull request body carrying the drift evidence and the comparison. Then it
stops. It does not move the champion pointer, it does not write a flag, and
it does not run the promotion gate's verdict, because the gate is judged on
a labelled **shadow** window (ADR 11) and a candidate that has never been in
shadow has no such window yet. The pull request says what is missing as
plainly as what is present.

**Nothing promotes itself.** That rule is the reason this module exists as a
separate step rather than as the tail of the trigger. The automated path
ends at a pull request a person merges, and `tests/test_retrain.py` holds
the job to it: after a run, the pointer file is byte for byte what it was.

**The candidate is fitted the way the champion was.** Same parameters, same
split in time, same refusal to train on a label that had not arrived
(ADR 10's rule 4, `models/train.split_by_time`). A candidate that beat the
champion by being fitted differently would say nothing about the drift that
asked for it.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from verdict.drift.trigger import Resolution, RetrainRequest, resolve
from verdict.models.champion import compare_models
from verdict.models.train import export_onnx, fit_champion, split_by_time


def retrain(
    request: RetrainRequest,
    *,
    track: str,
    table_path: Path,
    champion_path: Path,
    candidate_path: Path,
    train_share: float = 0.7,
    threads: int = 6,
) -> dict[str, Any]:
    """Fit a candidate on the arrived history and compare it to the champion.

    Args:
        request: The drift request that asked for this.
        track: `real` or `synthetic`, for the report and the split's rule.
        table_path: The history to fit on, replayed or staged.
        champion_path: The incumbent champion's ONNX file.
        candidate_path: Where the candidate's ONNX file goes.
        train_share: Where in the history the split falls. A later split is
            a later retrain: more of the history has arrived, and less is
            left to test on.
        threads: XGBoost threads. The live job fits with one, beside the
            scorer (ADR 28).

    Returns:
        The report: what asked for the retrain, what was fitted, and how the
        two compare on rows neither was trained on.
    """
    table = pq.read_table(table_path)
    split = split_by_time(table, train_share=train_share, wait_for_labels=track == "synthetic")
    last_trained_on = _last_transaction(table, split.train.event_ids)
    fitted = fit_champion(split.train, threads=threads)
    export_onnx(fitted, candidate_path, split.test.inputs[:5_000])
    comparison = compare_models(
        track,
        table_path,
        champion_path,
        candidate_path,
        train_share=train_share,
        challenger_prefix=candidate_path.stem,
    )
    beats = comparison["challenger_minus_champion"]["low"] > 0.0
    return {
        "opened_by": {
            "drift_on": request.opened_on.isoformat(),
            "quantities": list(request.quantities),
            "days_of_evidence": len(request.evidence),
            "by_hand": request.by_hand,
            "reason": request.reason,
        },
        "cutoff": split.cutoff.isoformat(),
        "train_share": train_share,
        "train": {
            "rows": len(split.train.labels),
            "frauds": int(split.train.labels.sum()),
            "excluded_unarrived_labels": split.train.excluded_unarrived,
        },
        "fit": {
            "params": fitted.params,
            "trees": fitted.best_iteration,
            "seconds": round(fitted.seconds, 1),
        },
        "comparison": comparison,
        "drifted_days_in_training": _drift_coverage(request, split.cutoff, last_trained_on),
        "beats_incumbent": beats,
        "request": str(resolve(request, candidate_beat_incumbent=beats)),
        "promoted": False,
    }


def _last_transaction(table: pa.Table, event_ids: tuple[str, ...]) -> dt.datetime:
    """The latest transaction a training set contains.

    Not the set's cutoff. The cutoff is the moment the model is as of, and a
    model as of a moment can only use transactions whose labels had arrived
    by then, which on this stream is a week earlier. On the synthetic track
    the model is also trained a label delay after its split point
    (`split_by_time`'s `wait_for_labels`), so the cutoff can sit well past the
    last transaction it learned from. Whether a candidate has seen a shift is
    a question about the transactions, so it is answered from them.

    Args:
        table: The table the set was drawn from.
        event_ids: The training set's events.

    Returns:
        The latest event time among them.

    Raises:
        ValueError: If the training set is empty.
    """
    kept = pc.is_in(table.column("event_id"), value_set=pa.array(event_ids))
    latest = pc.max(pc.filter(table.column("event_time"), kept)).as_py()
    if latest is None:
        msg = "the training set is empty"
        raise ValueError(msg)
    moment: dt.datetime = latest
    return moment


def _drift_coverage(
    request: RetrainRequest, cutoff: dt.datetime, last_trained_on: dt.datetime
) -> dict[str, Any]:
    """Whether the candidate could see the drift that asked for it.

    Drift is reported a day after it starts (ADR 12); a label takes a week to
    arrive (ADR 10). A candidate may only be fitted on labels that had
    arrived, so the first candidate a drift request can produce is usually
    fitted entirely on transactions from before the drift. The promotion gate
    makes that safe, because a candidate that has not seen the shift loses to
    the incumbent and nothing moves. It does not make it silent: a reviewer
    reading a losing candidate should know why it lost, and the request has
    to fire again once the shifted days' labels are in.

    Args:
        request: The drift request.
        cutoff: The moment the candidate is as of.
        last_trained_on: The latest transaction it was fitted on.

    Returns:
        The first drifted day, the last day trained on, and whether the
        training reaches the drift.
    """
    last_day = last_trained_on.date()
    if not request.evidence:
        # Opened by hand: there is no drifted day to reach.
        return {
            "first_drifted_day": None,
            "training_cutoff": cutoff.isoformat(),
            "last_day_trained_on": last_day.isoformat(),
            "covers_the_drift": None,
            "days_short": None,
        }
    first_drifted = min(report.day for report in request.evidence)
    return {
        "first_drifted_day": first_drifted.isoformat(),
        "training_cutoff": cutoff.isoformat(),
        "last_day_trained_on": last_day.isoformat(),
        "covers_the_drift": last_day >= first_drifted,
        "days_short": max(0, (first_drifted - last_day).days),
    }


def pull_request_body(report: dict[str, Any], request: RetrainRequest) -> str:
    """The pull request a person reads before deciding.

    It carries the drift that asked for the candidate, the candidate's
    offline comparison against the incumbent, and what is still missing. It
    does not ask for a merge: a candidate with no shadow window has not
    earned one.

    Args:
        report: What `retrain` returned.
        request: The drift request, for its evidence table.

    Returns:
        Markdown, plain punctuation.
    """
    comparison = report["comparison"]
    difference = comparison["challenger_minus_champion"]
    champion = comparison["champion_pr_auc"]
    candidate = comparison["challenger_pr_auc"]
    lines = [
        f"## Retraining candidate {comparison['challenger']}",
        "",
        f"Incumbent champion: `{comparison['champion']}`. Candidate: `{comparison['challenger']}`.",
        "",
        "**This pull request does not promote anything.** Merging it accepts the "
        "candidate as the challenger to run in shadow. The champion pointer moves "
        "only on a second pull request carrying the promotion gate's verdict on a "
        "labelled shadow window (ADR 11), which does not exist yet and cannot until "
        "the candidate has scored live traffic beside the champion.",
        "",
        "### Why this was built",
        "",
        request.to_markdown(),
        "### The candidate, offline",
        "",
        f"Fitted as of {report['cutoff']} on {report['train']['rows']:,} rows "
        f"({report['train']['frauds']:,} frauds; "
        f"{report['train']['excluded_unarrived_labels']:,} left out because their labels "
        f"had not arrived), {report['fit']['trees']:,} trees. Both models scored on the "
        f"same {comparison['test']['rows']:,} later rows, which neither was trained on.",
        "",
        "| Measure | Champion | Candidate | Difference (95% CI) |",
        "|---|---:|---:|---|",
        (
            f"| Test PR-AUC | {champion['value']:.4f} | {candidate['value']:.4f} | "
            f"{difference['value']:+.4f} ({difference['low']:+.4f} to "
            f"{difference['high']:+.4f}) |"
        ),
        "",
        f"Candidate model hop, single row: {comparison['model_hop_ms']['p50']:.3f} ms at p50, "
        f"{comparison['model_hop_ms']['p99']:.3f} ms at p99.",
        "",
        "### What is missing",
        "",
        "- A labelled shadow window, which the promotion gate needs and this does not have.",
        "- The gate's verdict on it, with its margins and latency check.",
        "- A person who has read both.",
        "",
        _coverage_note(report["drifted_days_in_training"]),
        "",
        _request_note(report["request"]),
        "",
    ]
    return "\n".join(lines)


def _coverage_note(coverage: dict[str, Any]) -> str:
    """One paragraph on whether the candidate has seen the drift.

    Args:
        coverage: What `_drift_coverage` returned.

    Returns:
        Markdown, plain punctuation.
    """
    if coverage["covers_the_drift"] is None:
        return (
            f"The candidate was fitted on transactions up to "
            f"{coverage['last_day_trained_on']}. The request was opened by hand, not by "
            f"drift, so there is no drifted day for it to reach."
        )
    if coverage["covers_the_drift"]:
        return (
            f"The candidate was fitted on transactions up to "
            f"{coverage['last_day_trained_on']}, which reaches the first drifted day "
            f"({coverage['first_drifted_day']}), so it has seen the shifted stream."
        )
    return (
        f"**The candidate has not seen the drift.** The latest transaction it was fitted on "
        f"is from {coverage['last_day_trained_on']}, and the first drifted day is "
        f"{coverage['first_drifted_day']}, {coverage['days_short']} days later. Drift is "
        f"reported a day after it starts and a label takes a week to arrive, so the first "
        f"candidate a request can produce is fitted entirely on the old regime. It is a "
        f"rebuild, not an answer. The promotion gate makes that safe, since a candidate "
        f"that has not seen the shift will not beat the incumbent; it is said here so a "
        f"losing result is not a puzzle, and so the request fires again once the shifted "
        f"days' labels arrive."
    )


def _request_note(resolution: str) -> str:
    """One line on whether the drift request this answers may close.

    Args:
        resolution: The request's resolution, as `retrain` recorded it.

    Returns:
        Markdown, plain punctuation.
    """
    if resolution == Resolution.ANSWERED:
        return (
            "The drift request is **answered**: this candidate beats the incumbent by an "
            "interval that excludes zero. It still reaches the pointer only through shadow "
            "and the gate."
        )
    return (
        "The drift request stays **open**: this candidate does not beat the incumbent, so "
        "the shift it was opened for has not been answered. It is asked again when more "
        "labelled history has arrived, and closes only when a candidate wins or the drift "
        "has stopped for two consecutive days."
    )


def write_pull_request(body: str, path: Path) -> Path:
    """Write the pull request body where a job or a person can pick it up.

    Args:
        body: The markdown.
        path: Where it goes.

    Returns:
        The path written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def request_from_report(report: dict[str, Any]) -> RetrainRequest:
    """Rebuild a drift request from a saved drift report.

    `verdict drift-report` writes JSON, and the retraining job takes the
    request the trigger made. This reads one back so the two commands can be
    run apart, on different machines or days.

    Args:
        report: A parsed `docs/drift-report.json`.

    Returns:
        The request the run's first firing made.

    Raises:
        ValueError: If the run opened no request, in which case there is
            nothing to retrain for.
    """
    from verdict.drift.monitors import DailyReport, QuantityResult, Status

    opened = report.get("first_request")
    if opened is None:
        msg = "the drift run opened no retraining request"
        raise ValueError(msg)
    evidence = tuple(
        DailyReport(
            day=dt.date.fromisoformat(day["day"]),
            results=tuple(
                QuantityResult(
                    name=result["name"],
                    values=result["values"],
                    psi=result["psi"],
                    ks_statistic=result["ks_statistic"],
                    ks_p_value=result["ks_p_value"],
                    status=Status(result["status"]),
                )
                for result in day["results"]
            ),
        )
        for day in opened["evidence"]
    )
    return RetrainRequest(
        opened_on=dt.date.fromisoformat(opened["opened_on"]),
        quantities=tuple(opened["quantities"]),
        evidence=evidence,
    )
