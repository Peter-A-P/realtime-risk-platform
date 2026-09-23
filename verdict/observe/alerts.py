"""From Prometheus's firing alerts to messages for Peter (ADR 26).

Prometheus evaluates the alert rules (`deploy/live/compose.yml`); this polls
its alerts API and writes one message when an alert starts firing, a
reminder while it keeps firing, and one when it ends. It writes them as
files in an outbox on the data volume and sends nothing itself: sending
needs the instance's credentials, which no container can reach (the
metadata hop limit, `deploy/terraform/compute.tf`), so a small loop on the
host publishes each file to the verdict-alerts SNS topic and deletes it
(`deploy/terraform/boot.sh.tftpl`).

What has been told is kept in a state file beside the outbox, so a spot
replacement neither repeats an alert nor forgets to say it ended.

If Prometheus cannot be read for ten minutes, that is an alert of its own,
`PrometheusUnreachable`, since every other alert depends on it; nothing
already told is called resolved while Prometheus cannot be seen.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

DASHBOARD: Final = "https://risk.peterparker.ca"
RUNBOOK: Final = "docs/failure-modes.md in the repository"
REMIND_EVERY: Final = dt.timedelta(hours=6)
"""How often an alert that keeps firing is repeated."""
UNREACHABLE_AFTER: Final = 10
"""Failed polls in a row, at one a minute, before Prometheus itself is the alert."""
UNREACHABLE: Final = "PrometheusUnreachable"


@dataclass(frozen=True, slots=True)
class Firing:
    """One alert firing now.

    Attributes:
        key: The alert's name and labels, which identify it across polls.
        name: The alert's name.
        summary: What it means, from the rule.
        labels: Its labels, for the message.
    """

    key: str
    name: str
    summary: str
    labels: Mapping[str, str]


def firing(response: Mapping[str, Any]) -> list[Firing]:
    """The firing alerts in a Prometheus `/api/v1/alerts` response.

    Pending alerts (true, but not for long enough yet) are left out.

    Args:
        response: The decoded JSON.

    Returns:
        One per firing alert.
    """
    found: list[Firing] = []
    for alert in response["data"]["alerts"]:
        if alert["state"] != "firing":
            continue
        labels = {str(k): str(v) for k, v in alert["labels"].items()}
        name = labels.get("alertname", "unnamed")
        key = name + "".join(f",{k}={labels[k]}" for k in sorted(labels) if k != "alertname")
        found.append(Firing(key, name, str(alert["annotations"].get("summary", "")), labels))
    return found


def _read_prometheus(url: str) -> Mapping[str, Any]:
    with urllib.request.urlopen(f"{url}/api/v1/alerts", timeout=10) as response:
        decoded: Mapping[str, Any] = json.load(response)
        return decoded


class Relay:
    """Turns polls of Prometheus into messages in the outbox."""

    def __init__(
        self,
        outbox: Path,
        state: Path,
        *,
        read: Callable[[], Mapping[str, Any]],
        now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    ) -> None:
        """Assemble the relay.

        Args:
            outbox: Where messages are written for the host to send.
            state: What has been told, kept across restarts.
            read: One poll of Prometheus's alerts API, decoded.
            now: The clock.
        """
        self.outbox = outbox
        self.state = state
        self.read = read
        self.now = now
        self.failures = 0
        self._sequence = 0

    def _told(self) -> dict[str, str]:
        if not self.state.exists():
            return {}
        told: dict[str, str] = json.loads(self.state.read_text(encoding="utf-8"))
        return told

    def _save(self, told: Mapping[str, str]) -> None:
        temporary = self.state.with_suffix(".tmp")
        temporary.write_text(json.dumps(told, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(self.state)

    def _write(self, subject: str, lines: Iterable[str]) -> None:
        """One message, as the input `aws sns publish --cli-input-json` takes."""
        self.outbox.mkdir(parents=True, exist_ok=True)
        self._sequence += 1
        stamp = self.now().strftime("%Y%m%dT%H%M%S")
        target = self.outbox / f"{stamp}-{self._sequence:04d}.json"
        temporary = target.with_suffix(".tmp")
        # SNS caps an email subject at 100 characters.
        body = {"Subject": subject[:100], "Message": "\n".join(lines) + "\n"}
        temporary.write_text(json.dumps(body), encoding="utf-8")
        temporary.replace(target)

    def poll(self) -> list[str]:
        """Read Prometheus once and write whatever needs telling.

        Returns:
            The subjects written, for the log.
        """
        now = self.now()
        blind = False
        try:
            alerts = firing(self.read())
            self.failures = 0
        except (OSError, ValueError, KeyError):
            self.failures += 1
            if self.failures < UNREACHABLE_AFTER:
                return []  # a blip: say nothing, and do not call anything resolved
            # Nothing that was firing can be said to have stopped: it cannot
            # be seen. Only that it cannot be seen is new.
            blind = True
            alerts = [
                Firing(
                    UNREACHABLE,
                    UNREACHABLE,
                    "Prometheus has not answered for ten minutes, so no other alert can fire.",
                    {},
                )
            ]
        told = self._told()
        written: list[str] = []
        current = {alert.key: alert for alert in alerts}
        for key, alert in current.items():
            since = told.get(key)
            if since is not None and now - dt.datetime.fromisoformat(since) < REMIND_EVERY:
                continue
            subject = f"verdict: {alert.name} {'still firing' if since else 'firing'}"
            self._write(
                subject,
                [
                    alert.summary,
                    "",
                    *(f"{k}: {v}" for k, v in sorted(alert.labels.items())),
                    f"at: {now.isoformat(timespec='seconds')}",
                    "",
                    f"Dashboard: {DASHBOARD}",
                    f"What to do: {RUNBOOK}, under {alert.name}.",
                ],
            )
            told[key] = now.isoformat()
            written.append(subject)
        for key in [k for k in told if k not in current and not blind]:
            name = key.split(",", 1)[0]
            subject = f"verdict: {name} resolved"
            self._write(subject, [f"{key} stopped firing at {now.isoformat(timespec='seconds')}."])
            del told[key]
            written.append(subject)
        self._save(told)
        return written


def run_forever(relay: Relay, *, every_seconds: float, stop: threading.Event) -> None:
    """Poll, wait, and again, until stopped.

    Args:
        relay: The relay.
        every_seconds: The wait between polls.
        stop: Set to stop.
    """
    while not stop.is_set():
        for subject in relay.poll():
            print(subject, flush=True)
        stop.wait(every_seconds)


def prometheus_reader(url: str) -> Callable[[], Mapping[str, Any]]:
    """A reader of one Prometheus's alerts API.

    Args:
        url: Prometheus's base URL.

    Returns:
        The reader.
    """
    return lambda: _read_prometheus(url)
