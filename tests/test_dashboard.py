"""The public dashboard: every panel asks for a metric the platform exports.

A panel whose metric was renamed shows "No data" to anyone who opens
risk.peterparker.ca, and nothing else would notice. So every query's metric
names are checked against what the platform's services actually register.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from verdict.history.compact import HistoryPaths
from verdict.history.compactor import CompactorMetrics
from verdict.history.labels import CollectorMetrics
from verdict.live.feed import Feed, FeedMetrics
from verdict.observe.dashboard import (
    DATASOURCE_UID,
    KPI_HEIGHT,
    dashboard,
    datasource,
    recorded,
    write_files,
)
from verdict.observe.metrics import ScorerMetrics

LIVE_COMPOSE = Path(__file__).resolve().parents[1] / "deploy" / "live" / "compose.yml"
_SUFFIXES = {"counter": ("_total",), "histogram": ("_bucket", "_sum", "_count"), "gauge": ("",)}


def exported() -> set[str]:
    names: set[str] = set()
    for registry in (
        ScorerMetrics().registry,
        FeedMetrics(Feed.TRANSACTIONS).registry,
        CollectorMetrics().registry,
        CompactorMetrics(HistoryPaths(Path("history"))).registry,
    ):
        for family in registry.collect():
            names |= {family.name + suffix for suffix in _SUFFIXES.get(family.type, ("",))}
    return names


def queries() -> list[str]:
    return [t["expr"] for p in dashboard()["panels"] for t in p.get("targets", [])]


def test_every_query_asks_for_a_metric_that_exists() -> None:
    wanted = {name for expr in queries() for name in re.findall(r"verdict_[a-z_]+", expr)}
    assert wanted, "the dashboard should query the platform's metrics"
    assert wanted <= exported(), wanted - exported()


def recordings() -> dict[str, dict[str, object]]:
    compose = yaml.safe_load(LIVE_COMPOSE.read_text(encoding="utf-8"))
    groups = yaml.safe_load(compose["configs"]["alert-rules"]["content"])["groups"]
    return {
        rule["record"]: {"interval": group.get("interval"), "expr": rule["expr"]}
        for group in groups
        for rule in group["rules"]
        if "record" in rule
    }


def test_every_recorded_series_the_dashboard_reads_is_recorded_on_the_live_stack() -> None:
    wanted = {name for expr in queries() for name in re.findall(r"le:verdict_[a-z_]+:\w+", expr)}
    assert wanted == {recorded("30d"), recorded("7d")}
    assert wanted <= set(recordings()), wanted - set(recordings())


def test_a_recording_counts_the_window_its_name_says() -> None:
    for window in ("30d", "7d"):
        rule = recordings()[recorded(window)]
        assert rule["expr"] == (
            f"sum by (le) (increase(verdict_event_to_decision_seconds_bucket[{window}]))"
        )
        assert rule["interval"] == "1m"


def test_the_headline_is_the_top_row_and_is_read_at_one_moment() -> None:
    panels = dashboard()["panels"]
    top = [p for p in panels if p["gridPos"]["y"] == 0]
    assert [p["title"] for p in top] == [
        "Event to decision, last 30 days",
        "Event to decision, last 7 days",
    ]
    for panel in top:
        assert [t["legendFormat"] for t in panel["targets"]] == ["median", "p95", "p99"]
        assert all(t["instant"] for t in panel["targets"])
    assert sum(p["gridPos"]["w"] for p in top) == 24
    assert all(p["gridPos"]["y"] >= KPI_HEIGHT for p in panels if p not in top)


def test_every_panel_reads_the_provisioned_data_source() -> None:
    assert datasource()["datasources"][0]["uid"] == DATASOURCE_UID
    for panel in dashboard()["panels"]:
        if "targets" in panel:
            assert panel["datasource"]["uid"] == DATASOURCE_UID


def test_the_dashboard_cannot_be_edited_by_its_viewers() -> None:
    assert dashboard()["editable"] is False


def test_the_words_on_the_dashboard_use_plain_punctuation() -> None:
    text = json.dumps(dashboard(), ensure_ascii=False)
    assert not set(text) & set("\u2013\u2014\u2018\u2019\u201c\u201d")


def test_the_files_grafana_loads_are_written_and_readable(tmp_path: Path) -> None:
    written = write_files(tmp_path, mounted_at="/etc/grafana/verdict")
    assert len(written) == 3
    provider = yaml.safe_load(
        (tmp_path / "provisioning" / "dashboards" / "verdict.yaml").read_text(encoding="utf-8")
    )
    assert provider["providers"][0]["options"]["path"] == "/etc/grafana/verdict/dashboards"
    model = json.loads((tmp_path / "dashboards" / "verdict.json").read_text(encoding="utf-8"))
    assert model["uid"] == dashboard()["uid"]


WINDOW = ("2026-09-29T19:11:00Z", "2026-10-29T19:11:00Z")


def test_the_archive_opens_on_the_whole_window_and_never_refreshes() -> None:
    model = dashboard(WINDOW)
    assert model["time"] == {"from": WINDOW[0], "to": WINDOW[1]}
    assert model["refresh"] == ""
    assert model["editable"] is False


def test_the_archive_takes_every_rate_over_grafanas_interval() -> None:
    live = [t["expr"] for p in dashboard()["panels"] for t in p.get("targets", [])]
    archived = [t["expr"] for p in dashboard(WINDOW)["panels"] for t in p.get("targets", [])]
    assert len(archived) == len(live)
    assert any("[5m]" in expr for expr in live)
    assert not any(re.search(r"\[[0-9]+m\]", expr) for expr in archived)
    raw = r"sum by (le) (increase(\1[\2]))"
    assert all(
        re.sub(
            r"le:(verdict_[a-z_]+_bucket):increase(\w+)",
            raw,
            re.sub(r"\[[0-9]+m\]", "[$__rate_interval]", a),
        )
        == b
        for a, b in zip(live, archived, strict=True)
    )


def test_the_archive_reads_its_headline_from_the_raw_buckets() -> None:
    """The archive's Prometheus runs no rules, and may hold none of their output."""
    archived = [t["expr"] for p in dashboard(WINDOW)["panels"] for t in p.get("targets", [])]
    assert not any("le:verdict" in expr for expr in archived)
    assert (
        "histogram_quantile(0.99, sum by (le) "
        "(increase(verdict_event_to_decision_seconds_bucket[30d])))" in archived
    )


def test_the_archive_says_the_stack_is_gone_in_plain_punctuation() -> None:
    text = json.dumps(dashboard(WINDOW), ensure_ascii=False)
    assert "torn down" in text
    assert not set(text) & set("\u2013\u2014\u2018\u2019\u201c\u201d")
