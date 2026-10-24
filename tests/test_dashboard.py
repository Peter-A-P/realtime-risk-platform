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
from verdict.observe.dashboard import DATASOURCE_UID, dashboard, datasource, write_files
from verdict.observe.metrics import ScorerMetrics

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
    assert all(
        re.sub(r"\[[0-9]+m\]", "[$__rate_interval]", a) == b
        for a, b in zip(live, archived, strict=True)
    )


def test_the_archive_says_the_stack_is_gone_in_plain_punctuation() -> None:
    text = json.dumps(dashboard(WINDOW), ensure_ascii=False)
    assert "torn down" in text
    assert not set(text) & set("\u2013\u2014\u2018\u2019\u201c\u201d")
