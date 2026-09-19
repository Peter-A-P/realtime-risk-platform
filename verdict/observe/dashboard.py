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
from pathlib import Path
from typing import Any, Final

import yaml

REPOSITORY: Final = "https://github.com/Peter-A-P/realtime-risk-platform"
DATASOURCE_UID: Final = "prometheus"
DASHBOARD_UID: Final = "verdict-live"

INTRODUCTION: Final = f"""\
**Every card transaction scored before the money moves.** This is the live
window of the Real-Time Fraud and Risk Decisioning Platform: synthetic card
transactions arriving at about 1,000 a second, each decided by a stream
consumer that serves its features from a point-in-time correct feature
engine. The fraud patterns shift on a schedule that was sealed before the
window opened and is revealed after it closes.

Everything here is the synthetic live track, measured on the one instance
that runs it. No real payment data is involved. Method, code and every
decision record: [{REPOSITORY}]({REPOSITORY}).
"""


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


def _quantile(q: float, metric: str, by: str = "") -> str:
    group = f"le, {by}" if by else "le"
    return f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[5m])))"


def dashboard() -> dict[str, Any]:
    """The dashboard model Grafana loads.

    Returns:
        The dashboard, as Grafana's JSON model.
    """
    age = "verdict_event_to_decision_seconds"
    panels: list[dict[str, Any]] = [
        {
            "type": "text",
            "title": "What this is",
            "gridPos": {"x": 0, "y": 0, "w": 24, "h": 5},
            "options": {"mode": "markdown", "content": INTRODUCTION},
        },
        _panel(
            "Decisions a second",
            "stat",
            [("sum(rate(verdict_decisions_total[1m]))", "decisions")],
            (0, 5, 6, 4),
            unit="reqps",
        ),
        _panel(
            "Event to decision, 99th percentile",
            "stat",
            [(_quantile(0.99, age), "p99")],
            (6, 5, 6, 4),
            unit="s",
            description="Over the last five minutes. The budget is 50 ms (ADR 9).",
        ),
        _panel(
            "Since the last decision",
            "stat",
            [("time() - verdict_last_decision_timestamp_seconds", "seconds")],
            (12, 5, 6, 4),
            unit="s",
            description="Grows if the scorer stops. A spot replacement shows here.",
        ),
        _panel(
            "Feed behind its clock",
            "stat",
            [("max(verdict_feed_lag_seconds)", "lag")],
            (18, 5, 6, 4),
            unit="s",
            description="How late the feeds are sending. Large while one catches up.",
        ),
        _panel(
            "Event to decision",
            "timeseries",
            [
                (_quantile(0.50, age), "p50"),
                (_quantile(0.95, age), "p95"),
                (_quantile(0.99, age), "p99"),
            ],
            (0, 9, 12, 8),
            unit="s",
            description=(
                "From a transaction's event time, when the feed sends it, to its "
                "decision. Five-minute windows."
            ),
        ),
        _panel(
            "Scorer hops, 99th percentile",
            "timeseries",
            [(_quantile(0.99, "verdict_hop_seconds", "hop"), "{{hop}}")],
            (12, 9, 12, 8),
            unit="s",
            description="Features, model, rules and hand-off, as the scorer times them.",
        ),
        _panel(
            "Decisions by action",
            "timeseries",
            [("sum by (action) (rate(verdict_decisions_total[5m]))", "{{action}}")],
            (0, 17, 12, 8),
            unit="reqps",
        ),
        _panel(
            "Flush and checkpoint per batch, 99th percentile",
            "timeseries",
            [(_quantile(0.99, "verdict_commit_seconds", "part"), "{{part}}")],
            (12, 17, 12, 8),
            unit="s",
            description="The platform's own per-batch cost (ADR 9).",
        ),
        _panel(
            "Records sent by the feeds",
            "timeseries",
            [("sum by (feed) (rate(verdict_feed_records_total[1m]))", "{{feed}}")],
            (0, 25, 12, 8),
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
            (12, 25, 12, 8),
            unit="reqps",
            description="Redeliveries the ledger turned away, and dead letters by reason.",
        ),
        _panel(
            "Model deciding",
            "timeseries",
            [("sum by (model_version) (rate(verdict_decisions_total[5m]))", "{{model_version}}")],
            (0, 33, 24, 6),
            unit="reqps",
            description="Which model version made the decisions. A rollback shows here.",
        ),
    ]
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
        "panels": panels,
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


def write_files(out: Path, *, mounted_at: str = "/etc/grafana/verdict") -> list[Path]:
    """Write everything Grafana is provisioned from.

    Args:
        out: The directory to write, mounted into Grafana at `mounted_at`.
        mounted_at: Where Grafana sees `out`.

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
        out / "dashboards" / "verdict.json": json.dumps(dashboard(), indent=2) + "\n",
    }
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return list(files)
