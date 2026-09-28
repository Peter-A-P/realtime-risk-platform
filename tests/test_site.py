"""The demo site: its numbers are the reports' numbers, and it asks for nothing off-origin.

The page is the first thing a stranger reads, so the ways it could go wrong
quietly are held here: a data file that has fallen behind the reports it was
exported from, a figure without its interval, a request to another origin the
host's content security policy would refuse, the dashboard link missing from
the top, or typographic punctuation the repository does not use.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

from verdict.observe.alerts import DASHBOARD
from verdict.publish.demo_site import REPORTS, host_headers, site_data

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"
DOCS = ROOT / "docs"

TYPOGRAPHIC = (0x2010, 0x2011, 0x2012, 0x2013, 0x2014, 0x2015, 0x2018, 0x2019, 0x201C, 0x201D)
"""Dashes and curly quotes, by code point so this file does not contain them either."""


def _intervals(node: object, path: str = "") -> list[tuple[str, dict[str, float]]]:
    """Every value-with-interval in the data, wherever it sits."""
    found = []
    if isinstance(node, dict):
        if {"value", "low", "high"} <= node.keys():
            found.append((path, node))
        for key, value in node.items():
            found.extend(_intervals(value, f"{path}.{key}"))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found.extend(_intervals(value, f"{path}[{index}]"))
    return found


def test_the_committed_data_file_is_a_fresh_export_of_the_reports() -> None:
    committed = json.loads((SITE / "results.json").read_text(encoding="utf-8"))
    assert committed == json.loads(json.dumps(site_data(DOCS))), (
        "site/results.json has fallen behind docs/: run `verdict site-export`"
    )


def test_every_report_the_page_reads_is_committed() -> None:
    for name in REPORTS:
        assert (DOCS / name).is_file(), name


def test_every_interval_contains_its_estimate() -> None:
    intervals = _intervals(site_data(DOCS))
    assert len(intervals) > 40
    for path, interval in intervals:
        assert interval["low"] <= interval["value"] <= interval["high"], path


def test_the_headline_figures_are_the_readmes() -> None:
    data = site_data(DOCS)
    base = next(test for test in data["load_tests"] if test["rate"] == 1000)
    assert round(base["p99_ms"]["value"], 1) == 8.4
    assert all(test["kept_up"] == test["runs"] for test in data["load_tests"])
    assert round(data["rollback_ms"]["value"], 1) == 7.8
    assert data["queue"]["difference_dollars"]["value"] == 97.46
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for figure in ("8.4 (8.3 to 8.4)", "7.8 (6.5 to 9.1)", "+$97.46", "(48.03 to 150.44)"):
        assert figure in readme, figure


def test_the_drift_chart_shows_a_quiet_week_then_every_change() -> None:
    drift = site_data(DOCS)["drift"]
    starts = [regime["starts_day"] for regime in drift["regimes"][1:]]
    by_day = {day["day"]: day for day in drift["days"]}
    assert all(not day["drifted"] for day in drift["days"] if day["day"] < starts[0])
    for start in starts:
        assert by_day[start]["drifted"], f"the change on day {start} is not flagged that day"
    assert drift["first_request_day"] == starts[0] + 1


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.sources: list[str] = []
        self.anchors: list[tuple[str, dict[str, str | None]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag in ("script", "img", "iframe"):
            self.sources.append(values.get("src") or "")
        if tag == "link":
            self.sources.append(values.get("href") or "")
        if tag == "a":
            self.anchors.append((values.get("href") or "", values))
        assert "style" not in values, f"an inline style on <{tag}> is refused by the policy"


def test_the_page_loads_nothing_from_another_origin() -> None:
    parser = _Links()
    parser.feed((SITE / "index.html").read_text(encoding="utf-8"))
    assert parser.sources
    for source in parser.sources:
        assert not re.match(r"^(https?:)?//", source), source
        assert (SITE / source).is_file(), source
    script = (SITE / "app.js").read_text(encoding="utf-8")
    assert ".innerHTML" not in script
    assert re.findall(r"fetch\(\"([^\"]+)\"", script) == ["results.json"]


def test_the_dashboard_is_the_first_link_in_the_page_body() -> None:
    html = (SITE / "index.html").read_text(encoding="utf-8")
    body = html[html.index("<main>") :]
    parser = _Links()
    parser.feed(body)
    first, attrs = parser.anchors[0]
    assert first == DASHBOARD
    assert attrs.get("id") == "dashboard-link"
    assert site_data(DOCS)["dashboard"] == DASHBOARD


def test_the_host_sends_a_strict_content_security_policy() -> None:
    policy = host_headers(SITE)["Content-Security-Policy"]
    directives = dict(part.strip().split(" ", 1) for part in policy.split(";"))
    for name in ("default-src", "script-src", "style-src", "connect-src", "font-src"):
        assert directives[name] == "'self'", name
    assert directives["frame-ancestors"] == "'none'"


@pytest.mark.parametrize("name", ["index.html", "app.js", "style.css", "staticwebapp.config.json"])
def test_plain_punctuation(name: str) -> None:
    text = (SITE / name).read_text(encoding="utf-8")
    for character in map(chr, TYPOGRAPHIC):
        assert character not in text, f"{name} has {character!r}"
