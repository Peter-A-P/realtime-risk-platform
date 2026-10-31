"""The public dashboard at risk.peterparker.ca, written as code.

Grafana is provisioned from files: a data source, a dashboard provider, and
the dashboard itself. They are generated here rather than kept as hand-edited
JSON, so that a test can check every query against the metrics the platform
actually exports (`tests/test_dashboard.py`): a panel whose metric was renamed
shows "No data" to the public, and nothing else would notice.

`verdict observe grafana --out DIR` writes the files; on the live stack a
one-shot job does that into a directory Grafana mounts (ADR 14).

The dashboard shows only what the platform measures live, from the synthetic
track. It says so in its first panel.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Final

import yaml

REPOSITORY: Final = "https://github.com/Peter-A-P/realtime-risk-platform"
DATASOURCE_UID: Final = "prometheus"
DASHBOARD_UID: Final = "verdict-live"

INTRODUCTION: Final = f"""\
**Every card transaction scored before the money moves.** The live window of the
Real-Time Fraud and Risk Decisioning Platform: synthetic card transactions at about
1,000 a second, each decided by a stream consumer serving point-in-time correct
features, while the fraud patterns shift on a schedule sealed before the window
opened. The synthetic live track, measured on the one instance that runs it; no real
payment data is involved. Method, code and every decision record:
[{REPOSITORY}]({REPOSITORY}).
"""

ARCHIVE_INTRODUCTION: Final = f"""\
**Every card transaction scored before the money moves.** This is the record
of the Real-Time Fraud and Risk Decisioning Platform's thirty-day live window,
from the metrics the live stack kept: synthetic card transactions decided by a
stream consumer serving point-in-time correct features, while the fraud
patterns shifted on a schedule sealed before the window opened.

The stack that produced it has been torn down; this is its Prometheus data,
served again unchanged. Everything here is the synthetic live track. Method,
code and every decision record: [{REPOSITORY}]({REPOSITORY}).
"""

_FIXED_WINDOW: Final = re.compile(r"\[[0-9]+m\]")

AGE: Final = "verdict_event_to_decision_seconds"

KPI_HEIGHT: Final = 5
"""The headline row's height, numbers over their trends: it sits above everything else."""

BUDGET_SECONDS: Final = 0.05
"""The event-to-decision budget (ADR 9), drawn on the headline's trends."""

_QUANTILES: Final = ((0.50, "median"), (0.95, "p95"), (0.99, "p99"))

INTRODUCTION_HEIGHT: Final = 4
"""Rows for the introduction: its one paragraph at laptop width, no blank below."""

HOURLY: Final = f"le:{AGE}_bucket:increase1h"
"""Each hour's event-to-decision bucket counts: recorded once a minute on the live
stack (`deploy/live/compose.yml`), backfilled hourly from 2026-09-01, and so also in
the archive, whose Prometheus runs no rules (ADR 14's addendum of 2026-10-31)."""


def _target(expr: str, legend: str) -> dict[str, Any]:
    return {
        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
        "expr": expr,
        "legendFormat": legend,
        "refId": legend[:1].upper() or "A",
    }


def _panel(
    title: str,
    kind: str,
    targets: list[tuple[str, str]],
    grid: tuple[int, int, int, int],
    *,
    unit: str = "short",
    description: str = "",
) -> dict[str, Any]:
    x, y, w, h = grid
    refs = [_target(expr, legend) for expr, legend in targets]
    for index, ref in enumerate(refs):
        ref["refId"] = chr(ord("A") + index)
    return {
        "type": kind,
        "title": title,
        "description": description,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
        "targets": refs,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {},
    }


CAUGHT_UP = (
    '(max(max_over_time(verdict_feed_lag_seconds{feed="transactions"}[5m])) < 1) '
    "and on() (sum(rate(verdict_decisions_total[5m])) <= 1.1 * "
    'sum(rate(verdict_feed_records_total{feed="transactions"}[5m])))'
)
"""The platform is current: the rule ADR 25's report ends a recovery on."""


def _quantile(q: float, metric: str, by: str = "") -> str:
    group = f"le, {by}" if by else "le"
    return f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[5m])))"


def _mean(metric: str) -> str:
    """The exact mean over five minutes: the histogram's sum over its count."""
    return f"sum(rate({metric}_sum[5m])) / sum(rate({metric}_count[5m]))"


def trailing(q: float, window: str) -> str:
    """A quantile of every decision over a trailing span, from the hourly recording.

    On each hour the subquery takes the newest count of the hour before it
    (the backfill's samples are a few minutes past the hour, the live rule's
    each minute), and the counts are summed: under a million samples a point,
    where the raw buckets over thirty days are 3.3 million.

    Args:
        q: The quantile.
        window: A Prometheus duration in whole hours or days, such as `30d`.

    Returns:
        The PromQL.
    """
    hours = f"sum_over_time(last_over_time({HOURLY}[1h])[{window}:1h])"
    return f"histogram_quantile({q}, sum by (le) ({hours}))"


def _drawn(expr: str) -> str:
    """A headline's trend cut at twice the budget, all but its last point.

    A sparkline is scaled to its data and cannot be given a log axis or a
    ceiling, so one slow stretch flattens the rest: the trailing thirty days
    held the failed first window's minutes-long catch-ups (ADR 31) until
    2026-10-29. Cut at 100 ms, the budget sits halfway up every trend. The
    last point, the number shown, is never cut.
    """
    last = "on() (vector(time()) > ${__to:date:seconds} - ${__interval_ms} / 1000)"
    return f"(clamp_max({expr}, {2 * BUDGET_SECONDS}) unless {last}) or ({expr} and {last})"


def _headline(window: str, days: int, x: int) -> dict[str, Any]:
    """One window's median, p95 and p99 under one title, each over its own trend."""
    panel = _panel(
        f"Event to decision, last {days} days",
        "stat",
        [(_drawn(trailing(q, window)), name) for q, name in _QUANTILES],
        (x, 0, 12, KPI_HEIGHT),
        unit="s",
        description=(
            f"Every decision of the last {days} days, from the transaction's event time "
            "to its decision, catch-ups after a restart included. Estimated within the "
            "scorer's histogram buckets: so far a median within 5 to 10 ms, a p95 within "
            "10 to 25 ms and a p99 within 25 to 50 ms. Green inside the 50 ms budget "
            f"(ADR 9). Behind each, how that figure moved over the last {days} days, "
            "drawn from 0 to 100 ms, so that the budget is halfway up; anything slower "
            "is drawn at the top. Synthetic live track."
        ),
    )
    panel["timeFrom"] = window
    panel["hideTimeOverride"] = True
    panel["interval"] = "6h" if days > 7 else "1h"
    panel["fieldConfig"]["defaults"] |= {
        "min": 0,
        "max": 2 * BUDGET_SECONDS,
        "decimals": 1,
        "color": {"mode": "thresholds"},
        "thresholds": {
            "mode": "absolute",
            "steps": [{"color": "green", "value": None}, {"color": "red", "value": BUDGET_SECONDS}],
        },
    }
    panel["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": "value",
        "graphMode": "area",
        "textMode": "value_and_name",
        "text": {"titleSize": 14, "valueSize": 44},
        "justifyMode": "center",
        "orientation": "vertical",
    }
    return panel


def dashboard(window: tuple[str, str] | None = None) -> dict[str, Any]:
    """The dashboard model Grafana loads.

    Args:
        window: For the archive, the live window's first and last moments (ISO
            8601). The dashboard then opens on the whole window, never
            refreshes, and every rate is taken over Grafana's own interval,
            since a five-minute rate drawn at a thirty-day zoom shows one
            five-minute sample of each step and not the step. The headline
            numbers are drawn over the whole window and end at its end.

    Returns:
        The dashboard, as Grafana's JSON model.
    """
    model = _live_dashboard()
    if window is None:
        return model
    for panel in model["panels"]:
        if panel["type"] == "text":
            panel["options"]["content"] = ARCHIVE_INTRODUCTION
        panel.pop("timeFrom", None)
        for target in panel.get("targets", []):
            target["expr"] = _FIXED_WINDOW.sub("[$__rate_interval]", target["expr"])
    model["title"] = "Verdict: the thirty-day live window"
    model["refresh"] = ""
    model["time"] = {"from": window[0], "to": window[1]}
    return model


def _live_dashboard() -> dict[str, Any]:
    headline = [_headline("30d", 30, 0), _headline("7d", 7, 12)]
    panels: list[dict[str, Any]] = [
        {
            "type": "text",
            "title": "What this is",
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": INTRODUCTION_HEIGHT},
            "options": {"mode": "markdown", "content": INTRODUCTION},
        },
        _panel(
            "Decisions a second",
            "stat",
            [("sum(rate(verdict_decisions_total[1m]))", "decisions")],
            (0, 4, 6, 4),
            unit="reqps",
        ),
        _panel(
            "Event to decision, p99, last five minutes",
            "stat",
            [(_quantile(0.99, AGE), "p99")],
            (6, 4, 6, 4),
            unit="s",
            description="Over the last five minutes. The budget is 50 ms (ADR 9).",
        ),
        _panel(
            "Since the last decision",
            "stat",
            [("time() - verdict_last_decision_timestamp_seconds", "seconds")],
            (12, 4, 6, 4),
            unit="s",
            description="Grows if the scorer stops. A spot replacement shows here.",
        ),
        _panel(
            "Feed behind its clock",
            "stat",
            [("max(verdict_feed_lag_seconds)", "lag")],
            (18, 4, 6, 4),
            unit="s",
            description="How late the feeds are sending. Large while one catches up.",
        ),
        _panel(
            "Event to decision",
            "timeseries",
            [
                (_quantile(0.50, AGE), "p50"),
                (_quantile(0.95, AGE), "p95"),
                (_quantile(0.99, AGE), "p99"),
                (_mean(AGE), "mean (exact)"),
            ],
            (0, 8, 12, 8),
            unit="s",
            description=(
                "From a transaction's event time, when the feed sends it, to its "
                "decision. Five-minute windows. Every minute, including a spot "
                "replacement's catch-up. The percentiles are estimated within "
                "histogram buckets, so a catch-up reads as the top of its bucket; "
                "the mean is exact."
            ),
        ),
        _panel(
            "Event to decision while caught up, 99th percentile",
            "timeseries",
            [(f"{_quantile(0.99, AGE)} and on() {CAUGHT_UP}", "p99")],
            (0, 38, 24, 6),
            unit="s",
            description=(
                "Only the five-minute windows in which the platform was current: the "
                "feed on time and the scorer deciding no more than 1.1 times what the "
                "feed sends, so not working off a backlog (ADR 25). Gaps are catch-ups. "
                "The live report leaves out only catch-ups after a spot reclaim AWS "
                "announced; this panel leaves out every catch-up, and says so."
            ),
        ),
        _panel(
            "Scorer hops, 99th percentile",
            "timeseries",
            [(_quantile(0.99, "verdict_hop_seconds", "hop"), "{{hop}}")],
            (12, 8, 12, 8),
            unit="s",
            description="Features, model, rules and hand-off, as the scorer times them.",
        ),
        _panel(
            "Decisions by action",
            "timeseries",
            [("sum by (action) (rate(verdict_decisions_total[5m]))", "{{action}}")],
            (0, 16, 12, 8),
            unit="reqps",
        ),
        _panel(
            "Flush and checkpoint per batch, 99th percentile",
            "timeseries",
            [(_quantile(0.99, "verdict_commit_seconds", "part"), "{{part}}")],
            (12, 16, 12, 8),
            unit="s",
            description="The platform's own per-batch cost (ADR 9).",
        ),
        _panel(
            "Records sent by the feeds",
            "timeseries",
            [("sum by (feed) (rate(verdict_feed_records_total[1m]))", "{{feed}}")],
            (0, 24, 12, 8),
            unit="reqps",
            description="Labels begin seven days after transactions (ADR 10).",
        ),
        _panel(
            "Duplicates and records set aside",
            "timeseries",
            [
                ("sum(rate(verdict_duplicates_total[5m]))", "duplicates"),
                ("sum by (reason) (rate(verdict_dead_letters_total[5m]))", "{{reason}}"),
            ],
            (12, 24, 12, 8),
            unit="reqps",
            description="Redeliveries the ledger turned away, and dead letters by reason.",
        ),
        _panel(
            "Scorer memory",
            "timeseries",
            [("max(verdict_scorer_resident_bytes)", "resident")],
            (0, 50, 24, 6),
            unit="bytes",
            description=(
                "The scorer's resident memory, on a 32 GB machine (ADR 31). It grows "
                "for the first day as the day-long windows fill, and should then level off."
            ),
        ),
        _panel(
            "Model deciding",
            "timeseries",
            [("sum by (model_version) (rate(verdict_decisions_total[5m]))", "{{model_version}}")],
            (0, 32, 24, 6),
            unit="reqps",
            description="Which model version made the decisions. A rollback shows here.",
        ),
        _panel(
            "Label collector behind the labels feed",
            "timeseries",
            [
                (
                    'max(verdict_feed_latest_due_timestamp_seconds{feed="labels"}) '
                    "- max(verdict_labels_latest_label_timestamp_seconds)",
                    "behind",
                )
            ],
            (0, 44, 12, 6),
            unit="s",
            description=(
                "How far the labels written to history trail the labels sent. The "
                "labels topic keeps a day; an alert fires at two hours (ADR 26)."
            ),
        ),
        _panel(
            "Oldest unsealed hour of history",
            "timeseries",
            [("verdict_history_unsealed_age_seconds", "{{spool}}")],
            (12, 44, 12, 6),
            unit="s",
            description=(
                "Time since the oldest hour not yet sealed ended. Minutes when the "
                "compactor keeps up; an alert fires at three hours (ADR 26)."
            ),
        ),
    ]
    for panel in panels:
        panel["gridPos"]["y"] += KPI_HEIGHT
    return {
        "uid": DASHBOARD_UID,
        "title": "Verdict: live fraud decisioning",
        "tags": ["verdict"],
        "timezone": "utc",
        "refresh": "30s",
        "time": {"from": "now-6h", "to": "now"},
        "editable": False,
        "graphTooltip": 1,
        "schemaVersion": 39,
        "panels": headline + panels,
    }


def datasource() -> dict[str, Any]:
    """Grafana's data source provisioning: the stack's Prometheus.

    Returns:
        The provisioning document.
    """
    return {
        "apiVersion": 1,
        "datasources": [
            {
                "name": "Prometheus",
                "uid": DATASOURCE_UID,
                "type": "prometheus",
                "access": "proxy",
                "url": "http://prometheus:9090",
                "isDefault": True,
                "editable": False,
                "jsonData": {"timeInterval": "15s", "queryTimeout": "10s"},
            }
        ],
    }


def provider(dashboards: str) -> dict[str, Any]:
    """Grafana's dashboard provisioning: load from a directory, read-only.

    Args:
        dashboards: The directory, as the Grafana container sees it.

    Returns:
        The provisioning document.
    """
    return {
        "apiVersion": 1,
        "providers": [
            {
                "name": "verdict",
                "type": "file",
                "disableDeletion": True,
                "allowUiUpdates": False,
                "options": {"path": dashboards},
            }
        ],
    }


def write_files(
    out: Path,
    *,
    mounted_at: str = "/etc/grafana/verdict",
    window: tuple[str, str] | None = None,
) -> list[Path]:
    """Write everything Grafana is provisioned from.

    Args:
        out: The directory to write, mounted into Grafana at `mounted_at`.
        mounted_at: Where Grafana sees `out`.
        window: For the archive, the live window it shows (see `dashboard`).

    Returns:
        The files written.
    """
    files = {
        out / "provisioning" / "datasources" / "prometheus.yaml": yaml.safe_dump(
            datasource(), sort_keys=False
        ),
        out / "provisioning" / "dashboards" / "verdict.yaml": yaml.safe_dump(
            provider(f"{mounted_at}/dashboards"), sort_keys=False
        ),
        out / "dashboards" / "verdict.json": json.dumps(dashboard(window), indent=2) + "\n",
    }
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return list(files)
