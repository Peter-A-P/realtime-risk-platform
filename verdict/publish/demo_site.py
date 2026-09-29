"""The demo site's data, gathered from the committed reports (ADR 30).

The site at `site/` is static: HTML, a stylesheet, a script and one JSON file
this module writes. Nothing on it is computed in the browser and nothing is
typed into the page by hand: every figure it shows is read here from a report
under `docs/` that the README also cites, so the page and the README cannot
disagree, and `tests/test_site.py` holds the page's file to a fresh export.

The live numbers are not here. They are on the dashboard the page links at
its top, and the live window's own report fills them in when it ends.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Final

from verdict.drift.monitors import KS_STATISTIC_DRIFT, PSI_DRIFT
from verdict.observe.alerts import DASHBOARD
from verdict.scoring.rules import DecisionRules
from verdict.store.features import FEATURE_SET

REPOSITORY: Final = "https://github.com/Peter-A-P/realtime-risk-platform"

LIVE_WINDOW_START: Final[str | None] = "2026-09-29T19:11:00Z"
"""When the live window began, as `deploy/go-live.sh` chose it (docs/STATE.md);
None while one is being restarted (ADR 31). The first window began
2026-09-28T08:51Z and was stopped on its second day."""

LIVE_WINDOW_DAYS: Final = 30
"""The window's length (ADR 31). The sealed schedule was derived for sixty days,
of which the window runs the first thirty."""

LOAD_RATES: Final[tuple[int, ...]] = (1000, 2000, 3000, 4000)

STANDING_QUEUE_MS: Final = 5.0
"""A run whose last tenth waits this much longer than its first had a queue
standing. A consumer that cannot keep up falls behind by seconds within a
20-second run, so the margin separates the two cases by orders of magnitude."""

REPORTS: Final[tuple[str, ...]] = (
    *(f"loadtest-live-{rate}.json" for rate in LOAD_RATES),
    "dry-run-report.json",
    "rollback-drill.json",
    "leak-inflation.json",
    "drift-report.json",
    "retrain.json",
    "retrain-later.json",
    "queue-eval.json",
    "champion-synthetic.json",
    "challenger-synthetic.json",
    "champion-real.json",
    "challenger-real.json",
    "sealed-schedule.json",
    "week1-measurement.json",
)
"""Every report the page reads, in `docs/`."""


def _load(docs: Path, name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((docs / name).read_text(encoding="utf-8"))
    return loaded


def _interval(summary: Mapping[str, Any], scale: float = 1.0) -> dict[str, float]:
    """An estimate and its 95 percent interval, whatever the report called them."""
    value = summary["mean"] if "mean" in summary else summary["value"]
    return {
        "value": round(float(value) * scale, 6),
        "low": round(float(summary["low"]) * scale, 6),
        "high": round(float(summary["high"]) * scale, 6),
    }


def _load_tests(docs: Path) -> list[dict[str, Any]]:
    tests = []
    for rate in LOAD_RATES:
        report = _load(docs, f"loadtest-live-{rate}.json")
        summary = report["summary"]
        runs = [
            {
                "early_p50_ms": round(run["backlog"]["early_p50"], 3),
                "late_p50_ms": round(run["backlog"]["late_p50"], 3),
                "p99_ms": round(run["end_to_end"]["p99"], 3),
            }
            for run in report["per_run"]
        ]
        tests.append(
            {
                "rate": rate,
                "runs": len(runs),
                "kept_up": sum(
                    run["late_p50_ms"] - run["early_p50_ms"] < STANDING_QUEUE_MS for run in runs
                ),
                "p50_ms": _interval(summary["end_to_end_ms"]["p50"]),
                "p95_ms": _interval(summary["end_to_end_ms"]["p95"]),
                "p99_ms": _interval(summary["end_to_end_ms"]["p99"]),
                "hops_p99_ms": {
                    hop: _interval(value["p99"]) for hop, value in summary["hops_ms"].items()
                },
                "per_run": runs,
                "measured_at": report["measured_at"],
            }
        )
    return tests


def _dry_run(docs: Path) -> dict[str, Any]:
    report = _load(docs, "dry-run-report.json")
    serving = report["latency_while_serving"]["daily_mean_95ci"]
    availability = report["availability"]
    reclaims = availability["spot_reclaims"]
    return {
        "first_minute": report["window"]["first_minute"],
        "last_minute": report["window"]["last_minute"],
        "hours": report["window"]["minutes"] / 60,
        "decisions": round(availability["decisions"]),
        "p50_ms": _interval(serving["p50_ms"]),
        "p95_ms": _interval(serving["p95_ms"]),
        "p99_ms": _interval(serving["p99_ms"]),
        "days": serving["p99_ms"]["runs"],
        "uptime_percent": round(availability["uptime_percent"], 2),
        "spot_reclaims": len(reclaims),
        "reclaims": [
            {
                "noticed_at": reclaim["noticed_at"],
                "recovered_at": reclaim["recovered_at"],
                "minutes": reclaim["recovery_minutes"],
                "minutes_without_decisions": reclaim["minutes_without_decisions"],
            }
            for reclaim in reclaims
        ],
        "median_recovery_minutes": statistics.median(
            reclaim["recovery_minutes"] for reclaim in reclaims
        ),
        "minutes_without_decisions": availability["minutes_without_decisions"],
        "other_stops": len(availability["other_stops"]),
    }


def _drift(docs: Path) -> dict[str, Any]:
    report = _load(docs, "drift-report.json")
    regimes = report["regime_days"]
    first = regimes[0]["starts_on"]

    def regime_of(day: str) -> str:
        name = regimes[0]["name"]
        for regime in regimes:
            if regime["starts_on"] <= day:
                name = regime["name"]
        return str(name)

    def day_number(day: str) -> int:
        from datetime import date

        return (date.fromisoformat(day) - date.fromisoformat(first)).days

    request = report["first_request"]
    return {
        "quantities": len(report["reference"]["quantities"]),
        "psi_threshold": PSI_DRIFT,
        "ks_threshold": KS_STATISTIC_DRIFT,
        "regimes": [
            {"name": regime["name"], "starts_day": day_number(regime["starts_on"])}
            for regime in regimes
        ],
        "days": [
            {
                "day": day_number(day["day"]),
                "regime": regime_of(day["day"]),
                "drifted": day["drifted"],
                "values": day["values"],
            }
            for day in report["days"]
        ],
        "first_request_day": day_number(request["opened_on"]),
    }


def _comparison(report: Mapping[str, Any]) -> dict[str, Any]:
    comparison = report["comparison"]
    return {
        "champion": _interval(comparison["champion_pr_auc"]),
        "candidate": _interval(comparison["challenger_pr_auc"]),
        "difference": _interval(comparison["challenger_minus_champion"]),
        "covers_the_drift": report["drifted_days_in_training"]["covers_the_drift"],
    }


def _models(docs: Path) -> dict[str, Any]:
    tracks = {}
    for track in ("synthetic", "real"):
        champion = _load(docs, f"champion-{track}.json")
        challenger = _load(docs, f"challenger-{track}.json")
        tracks[track] = {
            "champion": _interval(champion["test_pr_auc"]),
            "challenger": _interval(challenger["challenger_pr_auc"]),
            "difference": _interval(challenger["challenger_minus_champion"]),
        }
    return tracks


def _queue(docs: Path) -> dict[str, Any]:
    report = _load(docs, "queue-eval.json")
    caught = report["caught_per_analyst_hour_cents"]
    capacity = report["capacity"]
    # The evaluated team works around the clock: its daily reviews are its
    # analysts' hourly reviews times 24, so its analyst-hours a day are fixed,
    # and the team's figures are the per-hour difference and its interval
    # scaled by a constant. A year is 365 such days at the same rate, which
    # is an extrapolation from the days measured and is labelled as one.
    hours_a_day = capacity["reviews_per_day"] / capacity["reviews_per_analyst_hour"]

    def team(days: float) -> dict[str, float]:
        return {
            key: round(caught[name] / 100 * hours_a_day * days)
            for key, name in (("value", "difference"), ("low", "low"), ("high", "high"))
        }

    return {
        "days": report["days"],
        "transactions": report["scored_after_cutoff"],
        "queued": report["queued"],
        "queue_fraud_share": report["queue_fraud_share"],
        "reviews_per_day": report["capacity"]["reviews_per_day"],
        "analysts": report["capacity"]["analysts"],
        "reviews_per_analyst_hour": report["capacity"]["reviews_per_analyst_hour"],
        "review_cost_dollars": report["costs"]["review_cost_cents"] / 100,
        "recovery_rate": report["costs"]["recovery_rate"],
        "by_score_dollars": round(caught["by_score"] / 100, 2),
        "by_expected_loss_dollars": round(caught["by_expected_loss"] / 100, 2),
        "difference_dollars": {
            "value": round(caught["difference"] / 100, 2),
            "low": round(caught["low"] / 100, 2),
            "high": round(caught["high"] / 100, 2),
        },
        "analyst_hours_a_day": hours_a_day,
        "team_a_day_dollars": team(1),
        "team_a_month_dollars": team(365 / 12),
        "team_a_year_dollars": team(365),
    }


def site_data(docs: Path) -> dict[str, Any]:
    """Everything the demo page shows, from the reports in `docs`.

    Args:
        docs: The repository's `docs/` directory.

    Returns:
        The page's data, JSON-ready.
    """
    rollback = _load(docs, "rollback-drill.json")
    leak = _load(docs, "leak-inflation.json")
    sealed = _load(docs, "sealed-schedule.json")
    generator = _load(docs, "week1-measurement.json")
    rules = DecisionRules()
    return {
        "sources": [f"docs/{name}" for name in REPORTS],
        "repository": REPOSITORY,
        "dashboard": DASHBOARD,
        "live": {
            "start": LIVE_WINDOW_START,
            "days": LIVE_WINDOW_DAYS,
            "schedule_sha256": sealed["schedule_sha256"],
            "secret_sha256": sealed["secret_sha256"],
            "source_sha256": sealed["source_sha256"],
            "sealed_at": sealed["sealed_at"],
        },
        "budget_ms": 50,
        "fraud_share": {
            "value": generator["fraud_share"]["mean"],
            "low": generator["fraud_share"]["ci95"][0],
            "high": generator["fraud_share"]["ci95"][1],
        },
        "load_tests": _load_tests(docs),
        "dry_run": _dry_run(docs),
        "rollback_ms": _interval(rollback["seconds_to_old_champion"], scale=1000),
        "leak": {
            "rows_changed": leak["test_rows_whose_features_differ"],
            "test_rows": leak["test_rows"],
            "same_model_inflation": _interval(leak["same_model_leaky_minus_fixed_test_features"]),
        },
        "drift": _drift(docs),
        "retrain": {
            "at_the_alarm": _comparison(_load(docs, "retrain.json")),
            "after_labels": _comparison(_load(docs, "retrain-later.json")),
        },
        "models": _models(docs),
        "queue": _queue(docs),
        "rules": {
            "decline_at": rules.decline_at,
            "review_at": rules.review_at,
            "review_amount_dollars": rules.review_amount_cents / 100,
        },
        "features": [
            {"name": spec.name, "entity": spec.entity.value, "description": spec.description}
            for spec in FEATURE_SET
        ],
    }


def export(docs: Path, out: Path) -> dict[str, Any]:
    """Write the page's data file.

    Args:
        docs: The repository's `docs/` directory.
        out: The JSON file to write, `site/results.json`.

    Returns:
        What was written.
    """
    data = site_data(docs)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return data


def host_headers(root: Path) -> dict[str, str]:
    """The headers the host sends, read from the site's own host config.

    Args:
        root: The site directory.

    Returns:
        The global headers in `staticwebapp.config.json`.
    """
    config = json.loads((root / "staticwebapp.config.json").read_text(encoding="utf-8"))
    headers: dict[str, str] = config["globalHeaders"]
    return headers


def serve(root: Path, port: int) -> None:
    """Serve the site locally with the headers the host sends.

    A plain file server sends no content security policy, so it shows a page
    the host would partly refuse. This one sends the same headers the host's
    config names, so what works here works there.

    Args:
        root: The site directory.
        port: The local port.
    """
    headers = host_headers(root)

    class Handler(SimpleHTTPRequestHandler):
        def end_headers(self) -> None:
            for name, value in headers.items():
                self.send_header(name, value)
            super().end_headers()

    with ThreadingHTTPServer(("127.0.0.1", port), partial(Handler, directory=str(root))) as server:
        server.serve_forever()
