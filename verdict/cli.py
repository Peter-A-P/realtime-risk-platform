"""The `verdict` command line.

The plan's full command set is `up | down | replay | loadtest | parity |
drift-report | queue-eval | rollback-drill`. Week 1 ships the commands the
generator needs and the ones that make the sealed schedule checkable:

- `generate` writes events to the raw log and reports the rate achieved;
- `schedule show` prints the development schedule;
- `schedule hash` prints the three hashes that seal a schedule;
- `schedule seal` takes the commitment for the live window;
- `schedule verify` checks a revealed secret against a commitment.

The rest arrive in the week that builds them.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Annotated

import typer

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import (
    DEV_SCHEDULE,
    SealedCommitment,
    derive_schedule,
    source_fingerprint,
)
from verdict.events.rawlog import RawEventLog
from verdict.events.schema import SchemaFingerprint

app = typer.Typer(
    name="verdict",
    help="Real-time fraud and risk decisioning platform.",
    no_args_is_help=True,
    add_completion=False,
)

schedule_app = typer.Typer(
    name="schedule", help="The regime schedule and its seal.", no_args_is_help=True
)
app.add_typer(schedule_app)


@app.command()
def generate(
    out: Annotated[Path, typer.Option(help="Directory for the raw log.")] = Path("data/raw/dev"),
    events: Annotated[int, typer.Option(help="How many events to generate.")] = 100_000,
    rate: Annotated[float, typer.Option(help="Nominal events per second in stream time.")] = 1000.0,
    seed: Annotated[int, typer.Option(help="Generator seed.")] = 20270201,
    cards: Annotated[int, typer.Option(help="Cards in the entity graph.")] = 200_000,
    devices: Annotated[int, typer.Option(help="Devices in the entity graph.")] = 150_000,
    merchants: Annotated[int, typer.Option(help="Merchants in the entity graph.")] = 4_000,
    compress: Annotated[bool, typer.Option(help="Gzip the raw log.")] = False,
) -> None:
    """Generate events into the raw log and report the rate achieved.

    The rate reported is generation throughput in wall-clock seconds, which is
    what the week 1 target of 1,000 events per second refers to. The `--rate`
    option sets the density of the stream in event time, not the speed of the
    run.

    Args:
        out: Directory for the raw log.
        events: How many events to generate.
        rate: Nominal events per second in stream time.
        seed: Generator seed.
        cards: Cards in the entity graph.
        devices: Devices in the entity graph.
        merchants: Merchants in the entity graph.
        compress: Whether to gzip the raw log.
    """
    population = Population(cards=cards, devices=devices, merchants=merchants)
    started_graph = time.perf_counter()
    graph = EntityGraph.build(seed, population)
    graph_seconds = time.perf_counter() - started_graph

    config = GeneratorConfig(
        seed=seed, population=population, events_per_second=rate, schedule=DEV_SCHEDULE
    )
    generator = Generator(config, graph=graph)

    started = time.perf_counter()
    fraud = 0
    with RawEventLog(out, compress=compress) as log:
        for record in generator.stream(limit=events):
            log.append(record)
            fraud += record.truth.is_fraud
        counts = log.counts
    elapsed = time.perf_counter() - started

    typer.echo(
        json.dumps(
            {
                "events": counts.transactions,
                "fraud_events": fraud,
                "fraud_share": round(fraud / max(1, counts.transactions), 5),
                "graph_build_seconds": round(graph_seconds, 3),
                "generate_seconds": round(elapsed, 3),
                "events_per_second": round(counts.transactions / elapsed, 1),
                "out": str(out),
                "seed": seed,
                "schedule": DEV_SCHEDULE.name,
                "schedule_sha256": DEV_SCHEDULE.fingerprint(),
                "graph_sha256": graph.fingerprint(),
                "schema_sha256": SchemaFingerprint.compute().sha256,
            },
            indent=2,
        )
    )


@schedule_app.command("show")
def schedule_show() -> None:
    """Print the development schedule as canonical JSON."""
    typer.echo(json.dumps(json.loads(DEV_SCHEDULE.to_json()), indent=2))


@schedule_app.command("hash")
def schedule_hash() -> None:
    """Print the hashes that identify the schedule and its source.

    These are the week 1 deliverable: the schedule cannot change after this
    without the change being visible.
    """
    typer.echo(
        json.dumps(
            {
                "dev_schedule_name": DEV_SCHEDULE.name,
                "dev_schedule_sha256": DEV_SCHEDULE.fingerprint(),
                "regimes_source_sha256": source_fingerprint(),
                "schema_sha256": SchemaFingerprint.compute().sha256,
            },
            indent=2,
        )
    )


@schedule_app.command("seal")
def schedule_seal(
    secret: Annotated[str, typer.Option(prompt=True, hide_input=True, help="The sealed secret.")],
    window_days: Annotated[float, typer.Option(help="Length of the live window in days.")] = 87.0,
    name: Annotated[str, typer.Option(help="Name for the derived schedule.")] = "live",
    out: Annotated[Path, typer.Option(help="Where to write the commitment.")] = Path(
        "docs/sealed-schedule.json"
    ),
) -> None:
    """Commit to a live schedule without revealing it.

    Run this once, before go-live. It writes the three hashes and nothing
    else: the secret is not stored, printed or logged. Keep the secret
    somewhere it will survive the live window, because the schedule cannot be
    reconstructed without it.

    Args:
        secret: The sealed secret.
        window_days: Length of the live window in days.
        name: Name for the derived schedule.
        out: Where to write the commitment.
    """
    commitment = SealedCommitment.seal(
        secret, window_days=window_days, name=name, now=dt.datetime.now(dt.UTC)
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(commitment.to_document(), encoding="utf-8")
    typer.echo(f"sealed {name} over {window_days} days, commitment written to {out}")
    typer.echo("the secret was not written anywhere; store it yourself")


@schedule_app.command("verify")
def schedule_verify(
    secret: Annotated[str, typer.Option(prompt=True, hide_input=True, help="The revealed secret.")],
    commitment_path: Annotated[
        Path, typer.Option("--commitment", help="The committed hashes.")
    ] = Path("docs/sealed-schedule.json"),
    reveal: Annotated[bool, typer.Option(help="Also print the derived schedule.")] = False,
) -> None:
    """Check a revealed secret against the committed hashes.

    This is the Jul 1 2027 operation, and the one a stranger runs to check
    that the live drift results were not arranged after the fact.

    Args:
        secret: The revealed secret.
        commitment_path: Path to the committed hashes.
        reveal: Whether to print the derived schedule as well.

    Raises:
        typer.Exit: With code 1 if the secret does not match the commitment.
    """
    commitment = SealedCommitment.model_validate_json(commitment_path.read_text(encoding="utf-8"))
    if not commitment.verify(secret):
        typer.echo("REFUSED: the secret, the derived schedule or regimes.py does not match")
        raise typer.Exit(code=1)
    typer.echo("verified: secret, derived schedule and regimes.py all match the commitment")
    if reveal:
        schedule = derive_schedule(
            secret, window_days=commitment.window_days, name=commitment.schedule_name
        )
        typer.echo(json.dumps(json.loads(schedule.to_json()), indent=2))


if __name__ == "__main__":  # pragma: no cover
    app()
