"""The `verdict` command line.

The plan's full command set is `up | down | replay | loadtest | parity |
drift-report | queue-eval | rollback-drill`. Week 1 ships the commands the
generator needs and the ones that make the sealed schedule checkable:

- `generate` writes events to the raw log and reports the rate achieved;
- `schedule show` prints the development schedule;
- `schedule hash` prints the three hashes that seal a schedule;
- `schedule seal` takes the commitment for the live window;
- `schedule verify` checks a revealed secret against a commitment.

The real-data track adds `data ingest | manifest | verify | inspect` for the
competition files, `data events` to map them onto the platform's events, and
`data check` to run the point-in-time check over that replay.

Week 4 adds `loadtest`, which drives the scorer at a fixed rate and reports
latency per hop; `serve`, the synchronous endpoint for the demo; and
`flag show | set | rollback`, the champion pointer the scorer reads per event.

The rest arrive in the week that builds them.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Annotated

import typer

from verdict.events import ieee_cis, ieee_cis_events
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
from verdict.scoring.model import Model

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

DATA_ENV = "VERDICT_IEEE_CIS_DIR"
"""Where the competition files are, when not at the default.

The files may not live in the repository's own tree: this one is under a
synced folder on the build machine, and a gigabyte of licensed data has no
business being synced. Every `data` command reads this before its default.
"""

data_app = typer.Typer(
    name="data", help="The real-data track's files and what is in them.", no_args_is_help=True
)
app.add_typer(data_app)


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
    window_days: Annotated[float, typer.Option(help="Length of the live window in days.")] = 60.0,
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


@data_app.command("ingest")
def data_ingest(
    archive: Annotated[Path, typer.Option(help="The downloaded competition archive.")],
    out: Annotated[Path, typer.Option(help="Where to extract it.", envvar=DATA_ENV)] = (
        ieee_cis.DEFAULT_DESTINATION
    ),
) -> None:
    """Extract the competition archive and record a hash of everything in it.

    Nothing here downloads anything. The competition rules permit use by
    people who have accepted them, so the download stays a deliberate act by
    the account holder, and this project stores no Kaggle credential.

    Args:
        archive: The downloaded archive.
        out: Where to extract it.
    """
    manifest = ieee_cis.ingest(archive, out)
    typer.echo(
        json.dumps(
            {
                "archive": manifest.archive_name,
                "archive_sha256": manifest.archive_sha256,
                "destination": manifest.destination,
                "files": [
                    {"name": entry.name, "mb": round(entry.bytes_written / 1e6, 1)}
                    for entry in manifest.files
                ],
            },
            indent=2,
        )
    )


@data_app.command("manifest")
def data_manifest(
    directory: Annotated[
        Path, typer.Option(help="Where the files already are.", envvar=DATA_ENV)
    ] = (ieee_cis.DEFAULT_DESTINATION),
    archive: Annotated[Path | None, typer.Option(help="The archive, if kept.")] = None,
) -> None:
    """Record a hash of files that were extracted outside this tool.

    Args:
        directory: Where the files are.
        archive: The archive they came from, if it is still around.
    """
    manifest = ieee_cis.manifest_existing(directory, archive)
    typer.echo(
        json.dumps(
            {
                "destination": manifest.destination,
                "archive_sha256": manifest.archive_sha256,
                "files": [
                    {
                        "name": entry.name,
                        "mb": round(entry.bytes_written / 1e6, 1),
                        "sha256": entry.sha256,
                    }
                    for entry in manifest.files
                ],
            },
            indent=2,
        )
    )


@data_app.command("verify")
def data_verify(
    directory: Annotated[Path, typer.Option(help="Where the files are.", envvar=DATA_ENV)] = (
        ieee_cis.DEFAULT_DESTINATION
    ),
) -> None:
    """Check the local files against the committed checksum record.

    Args:
        directory: Where the files are.

    Raises:
        typer.Exit: With code 1 if anything is missing or has changed.
    """
    result = ieee_cis.verify(directory)
    typer.echo(
        json.dumps(
            {
                "ok": result.ok,
                "checked": list(result.checked),
                "missing": list(result.missing),
                "mismatched": list(result.mismatched),
            },
            indent=2,
        )
    )
    if not result.ok:
        raise typer.Exit(code=1)


@data_app.command("inspect")
def data_inspect(
    directory: Annotated[
        Path, typer.Option(help="Where the files were extracted.", envvar=DATA_ENV)
    ] = (ieee_cis.DEFAULT_DESTINATION),
    full: Annotated[bool, typer.Option(help="Print every column, not just the findings.")] = False,
) -> None:
    """Report what the competition files contain.

    The loader that maps this data onto the platform's event schema is
    written against this report rather than against a memory of a schema
    published in 2019.

    Args:
        directory: Where the files were extracted.
        full: Whether to print the whole column list.
    """
    findings = ieee_cis.find_answers(directory)
    report: dict[str, object] = {
        "has_merchant_identifier": findings.has_merchant_identifier,
        "merchant_columns": list(findings.merchant_columns),
        "identity_coverage": findings.identity_coverage,
        "fraud_share": findings.fraud_share,
        "transaction_time": findings.time_column,
        "notes": list(findings.notes),
    }
    if full:
        schema = ieee_cis.inspect_file(directory / ieee_cis.TRANSACTION_FILE)
        report["rows"] = schema.rows
        report["columns"] = [
            {
                "name": column.name,
                "dtype": column.dtype,
                "missing": column.missing_share,
                "distinct_in_sample": column.distinct_in_sample,
            }
            for column in schema.columns
        ]
    typer.echo(json.dumps(report, indent=2))


@data_app.command("events")
def data_events(
    out: Annotated[Path, typer.Option(help="Where to write the event log, under data/.")] = Path(
        "data/raw/ieee-cis-events"
    ),
    directory: Annotated[
        Path, typer.Option(help="Where the competition files are.", envvar=DATA_ENV)
    ] = ieee_cis.DEFAULT_DESTINATION,
) -> None:
    """Map the competition's transactions onto the platform's events.

    Writes a transaction log and a label log, and prints counts only. The
    logs are a row-by-row copy of licensed data and stay under `data/`.

    Args:
        out: Where to write the logs.
        directory: Where the competition files are.
    """
    report = ieee_cis_events.write_event_log(ieee_cis_events.iter_records(directory), out)
    typer.echo(report.to_json())


@data_app.command("check")
def data_check(
    directory: Annotated[
        Path, typer.Option(help="Where the competition files are.", envvar=DATA_ENV)
    ] = ieee_cis.DEFAULT_DESTINATION,
    per_mille: Annotated[int, typer.Option(help="Cards per thousand checked in full.")] = 20,
) -> None:
    """Run the point-in-time check over the real-data replay.

    Every feature this track can compute is served by the engine for every
    event, and for a deterministic sample of cards every row is compared with
    the definition recomputed from that card's history. Prints counts only.

    Args:
        directory: Where the competition files are.
        per_mille: Cards per thousand to check.

    Raises:
        typer.Exit: With code 1 if any served value disagrees with the
            definition.
    """
    from verdict.features.replay_check import check_replay

    specs = ieee_cis_events.features_on_track()
    started = time.perf_counter()
    events = (record.event for record in ieee_cis_events.iter_records(directory))
    result = check_replay(events, specs, per_mille=per_mille)
    typer.echo(
        json.dumps(
            {
                "track": "real data (IEEE-CIS), offline",
                "events_replayed": result.events,
                "features": [spec.name for spec in specs],
                "cards_sampled": result.entities_sampled,
                "rows_of_sampled_cards": result.events_kept,
                "comparisons": result.report.rows_checked,
                "violations": len(result.report.violations),
                "seconds": round(time.perf_counter() - started, 1),
                "summary": result.report.summary(),
            },
            indent=2,
        )
    )
    if not result.report.clean:
        raise typer.Exit(code=1)


@app.command()
def loadtest(
    stream: Annotated[str, typer.Option(help="memory, or redpanda for the compose broker.")] = (
        "memory"
    ),
    rate: Annotated[float, typer.Option(help="Transactions sent per second.")] = 1000.0,
    events: Annotated[int, typer.Option(help="Transactions per run.")] = 20_000,
    runs: Annotated[int, typer.Option(help="Runs, for the interval. At least 2.")] = 5,
    warmup: Annotated[int, typer.Option(help="Decisions per run left out.")] = 1_000,
    bootstrap: Annotated[
        str | None, typer.Option(help="Broker address, for redpanda. Defaults to the host's.")
    ] = None,
    out: Annotated[Path | None, typer.Option(help="Also write the report here.")] = None,
) -> None:
    """Drive the scorer at a fixed rate and report latency per hop.

    Synthetic live track only. The model is the week 4 stand-in, and the
    report says so in its own fields.

    Args:
        stream: Which stream to run on.
        rate: Target sends per second.
        events: Transactions per run.
        runs: How many runs.
        warmup: Decisions per run excluded from the statistics.
        bootstrap: The broker's address. From inside the compose network it
            is `redpanda:9092`, which avoids the host's port forwarder (ADR 9).
        out: Where to write the JSON report, if anywhere.

    Raises:
        typer.BadParameter: If the stream is not one this build knows.
    """
    import platform
    from dataclasses import asdict

    from verdict.scoring import loadtest as load

    match stream:
        case "memory":
            backend = load.memory_backend()
        case "redpanda":
            backend = load.redpanda_backend(bootstrap)
        case _:
            msg = f"unknown stream {stream!r}; use memory or redpanda"
            raise typer.BadParameter(msg)

    sent = load.generate_events(events, rate=rate)
    results = [load.run_once(backend, sent, rate=rate, warmup=warmup) for _ in range(runs)]
    report = {
        "track": "synthetic live, local",
        "stream": backend.name,
        "model": "stand-in-0 (not trained; see verdict/scoring/model.py)",
        "features": "served by the in-process engine",
        "load_producer_ran_in": results[0].producer,
        "bootstrap": bootstrap if stream == "redpanda" else None,
        "rate_target_per_second": rate,
        "events_per_run": events,
        "warmup_decisions_excluded": warmup,
        "runs": runs,
        "host": {
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "system": f"{platform.system()} {platform.release()}",
        },
        "measured_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "summary": load.summarise(results),
        "per_run": [asdict(result) for result in results],
    }
    text = json.dumps(report, indent=2)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    typer.echo(text)


@app.command()
def score(
    bootstrap: Annotated[str | None, typer.Option(help="Broker address.")] = None,
    group: Annotated[str, typer.Option(help="Consumer group, where progress is kept.")] = (
        "scorer"
    ),
    flag: Annotated[
        Path | None, typer.Option(help="Follow this champion pointer; else the stand-in.")
    ] = None,
    metrics_port: Annotated[
        int, typer.Option(help="Serve Prometheus metrics on this port; 0 for none.")
    ] = 9108,
    metrics_host: Annotated[str, typer.Option(help="Interface the metrics listen on.")] = (
        "127.0.0.1"
    ),
) -> None:
    """Run the stream scorer until interrupted: the platform's decision path.

    Consumes `transactions` from the local broker, writes `decisions`, sets
    aside what it cannot decide on `dead-letter`, and checkpoints after every
    durable batch (ADR 8). Ctrl+C, or SIGTERM where the platform delivers it,
    stops it after the batch in hand. Metrics listen on localhost by default:
    nothing about the scoring path is meant to be public (`PLAN.md` section 8).

    A scorer that starts cold serves every card "no history" until its
    windows refill; ADR 8 records that as a known gap until the live stack's
    recovery work.

    Args:
        bootstrap: Broker address. Defaults to the compose stack's.
        group: The consumer group.
        flag: A champion pointer to follow.
        metrics_port: Where to serve metrics, or 0 for nowhere.
        metrics_host: The interface metrics listen on.
    """
    import signal
    import threading

    from prometheus_client import start_http_server

    from verdict.features.engine import FeatureEngine
    from verdict.observe.metrics import ScorerMetrics
    from verdict.scoring import service
    from verdict.scoring.consumer import StreamScorer
    from verdict.scoring.core import Decider, EngineFeatures
    from verdict.scoring.flags import FlaggedModels
    from verdict.scoring.model import FixedModel, ModelSource, StandInModel
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    models: ModelSource = (
        FixedModel(StandInModel()) if flag is None else FlaggedModels(flag, _known_models())
    )
    metrics = ScorerMetrics()
    stream = RedpandaStream(bootstrap or DEFAULT_BOOTSTRAP)
    scorer = StreamScorer(
        stream,
        decider=Decider(features=EngineFeatures(FeatureEngine()), models=models),
        group=group,
        on_decided=metrics.on_decided,
    )
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    stop = threading.Event()

    def ask_to_stop(signum: int, frame: object) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGINT, ask_to_stop)
    signal.signal(signal.SIGTERM, ask_to_stop)
    typer.echo(
        f"scoring from {stream.bootstrap} as group {group!r}"
        + (f", metrics on {metrics_host}:{metrics_port}" if metrics_port else "")
    )
    try:
        summary = service.run(scorer, stop=stop, metrics=metrics)
    finally:
        stream.close()
    typer.echo(
        f"stopped after {summary.polls} polls: {summary.decided} decided, "
        f"{summary.records} records read, set aside {summary.set_aside or 'none'}"
    )


@app.command(name="http-loadtest")
def http_loadtest(
    rate: Annotated[float, typer.Option(help="Transactions offered per second.")] = 1000.0,
    events: Annotated[int, typer.Option(help="Transactions per run.")] = 20_000,
    runs: Annotated[int, typer.Option(help="Runs, for the interval. At least 2.")] = 5,
    warmup: Annotated[int, typer.Option(help="Exchanges per run left out.")] = 1_000,
    connections: Annotated[int, typer.Option(help="Connections carrying the load.")] = 1,
    stream: Annotated[str, typer.Option(help="Where the endpoint writes: none or redpanda.")] = (
        "none"
    ),
    port: Annotated[int, typer.Option(help="Port the endpoint listens on.")] = 8099,
    durable: Annotated[bool, typer.Option(help="Flush each decision before responding.")] = True,
    out: Annotated[Path | None, typer.Option(help="Also write the report here.")] = None,
) -> None:
    """Drive the HTTP endpoint at a fixed rate, the other half of Rule C candidate 3.

    Starts the endpoint in its own process, loads it from this one, and stops
    it. The report carries the same statistics as `loadtest` so that the two
    transports can be set beside each other, plus what is particular to this
    one: how long a transaction waited for a free connection, and what the
    endpoint refused.

    Synthetic live track only, with the week 4 stand-in model.

    Args:
        rate: Target transactions offered per second.
        events: Transactions per run.
        runs: How many runs.
        warmup: Exchanges per run excluded from the statistics.
        connections: How many connections carry the load. One cannot exceed
            one transaction per round trip; several deliver out of order, and
            the engine refuses what arrives late.
        stream: Where the endpoint writes decisions.
        port: The port to listen on.
        durable: Whether the endpoint flushes each decision before responding.
        out: Where to write the JSON report, if anywhere.

    Raises:
        typer.BadParameter: If the stream is not one this build knows.
    """
    import platform
    from dataclasses import asdict

    from verdict.scoring import httpload
    from verdict.scoring import loadtest as load

    if stream not in {"none", "redpanda"}:
        msg = f"unknown stream {stream!r}; use none or redpanda"
        raise typer.BadParameter(msg)

    sent = load.generate_events(events, rate=rate)
    results = []
    for run in range(runs):
        # A fresh endpoint per run, on its own port. The decider's ledger and
        # the engine's windows are per process, and every run sends the same
        # transactions: a shared server would refuse the second run entirely
        # as already decided. The stream test makes fresh topics and a fresh
        # group per run for the same reason.
        with httpload.a_server(port=port + run, stream=stream, durable=durable) as base:
            results.append(
                httpload.run_once(base, sent, rate=rate, warmup=warmup, connections=connections)
            )
    report = {
        "track": "synthetic live, local",
        "transport": "http",
        "endpoint_writes_to": stream,
        "endpoint_flushes_before_responding": durable,
        "connections": connections,
        "model": "stand-in-0 (not trained; see verdict/scoring/model.py)",
        "features": "served by the in-process engine",
        "rate_target_per_second": rate,
        "events_per_run": events,
        "warmup_exchanges_excluded": warmup,
        "runs": runs,
        "host": {
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "system": f"{platform.system()} {platform.release()}",
        },
        "measured_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "summary": httpload.summarise(results),
        "per_run": [asdict(result) for result in results],
    }
    text = json.dumps(report, indent=2)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    typer.echo(text)


@app.command(name="flush-probe")
def flush_probe(
    bootstrap: Annotated[str | None, typer.Option(help="Broker address.")] = None,
    connections: Annotated[int, typer.Option(help="Producers opened, one after another.")] = 10,
    out: Annotated[Path | None, typer.Option(help="Also write the report here.")] = None,
) -> None:
    """Time the scorer's flush on a series of fresh producer connections.

    Not part of the platform: a measuring instrument for the path between
    this host and the broker. `verdict/stream/probe.py` says what it found
    and how to run the same thing from inside the broker's network, which is
    the comparison that makes the number mean anything.

    Args:
        bootstrap: Broker address. Defaults to the compose stack's.
        connections: How many producers to open in turn.
        out: Where to write the JSON report, if anywhere.
    """
    import platform
    from dataclasses import asdict

    from verdict.stream import probe as flushes
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP

    address = bootstrap or DEFAULT_BOOTSTRAP
    results = flushes.flush_by_connection(address, connections=connections)
    report = {
        "track": "synthetic live, local",
        "bootstrap": address,
        "host": {
            "machine": platform.machine(),
            "python": platform.python_version(),
            "system": f"{platform.system()} {platform.release()}",
        },
        "measured_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "summary": flushes.summarise(results),
        "per_connection": [asdict(result) for result in results],
    }
    text = json.dumps(report, indent=2)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    typer.echo(text)


flag_app = typer.Typer(
    name="flag", help="The champion pointer the scorer reads per event.", no_args_is_help=True
)
app.add_typer(flag_app)

DEFAULT_FLAG = Path("data/flags/champion.json")
"""Runtime state, not configuration: under `data/`, which git ignores."""


def _known_models() -> dict[str, Model]:
    """The models this build can score with, by version.

    Week 4 has one. Week 5 adds the champion and challenger, and this is where
    they are registered.

    Returns:
        Version to model.
    """
    from verdict.scoring.model import StandInModel

    stand_in = StandInModel()
    return {stand_in.version: stand_in}


@flag_app.command("show")
def flag_show(
    path: Annotated[Path, typer.Option(help="The flag file.")] = DEFAULT_FLAG,
) -> None:
    """Print the champion pointer.

    Args:
        path: The flag file.
    """
    from verdict.scoring.flags import FlagError, read_pointer

    try:
        typer.echo(read_pointer(path).to_json().rstrip())
    except FlagError as error:
        typer.echo(f"REFUSED: {error}", err=True)
        raise typer.Exit(code=1) from error


@flag_app.command("set")
def flag_set(
    version: Annotated[str, typer.Argument(help="The model version to score with.")],
    path: Annotated[Path, typer.Option(help="The flag file.")] = DEFAULT_FLAG,
) -> None:
    """Point the scorer at a model. For the drill and for applying a merged promotion.

    Promotion itself is a pull request carrying the shadow evidence; this only
    applies the decision that pull request made.

    Args:
        version: The model version.
        path: The flag file.
    """
    from verdict.scoring.flags import FlagError, set_champion

    try:
        typer.echo(set_champion(path, version, _known_models()).to_json().rstrip())
    except FlagError as error:
        typer.echo(f"REFUSED: {error}", err=True)
        raise typer.Exit(code=1) from error


@flag_app.command("rollback")
def flag_rollback(
    path: Annotated[Path, typer.Option(help="The flag file.")] = DEFAULT_FLAG,
) -> None:
    """Return the scorer to the previous champion, effective on its next event.

    Args:
        path: The flag file.
    """
    from verdict.scoring.flags import FlagError, rollback

    try:
        typer.echo(rollback(path).to_json().rstrip())
    except FlagError as error:
        typer.echo(f"REFUSED: {error}", err=True)
        raise typer.Exit(code=1) from error


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Interface to listen on.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to listen on.")] = 8000,
    flag: Annotated[
        Path | None, typer.Option(help="Follow this champion pointer; else the stand-in.")
    ] = None,
) -> None:
    """Serve the synchronous scoring endpoint, for the demo.

    Listens on localhost by default: nothing in this platform's scoring path
    is meant to be publicly writable (`PLAN.md` section 8).

    Args:
        host: Interface to listen on.
        port: Port to listen on.
        flag: A champion pointer to follow.
    """
    import uvicorn

    from verdict.features.engine import FeatureEngine
    from verdict.scoring.core import Decider, EngineFeatures
    from verdict.scoring.flags import FlaggedModels
    from verdict.scoring.http_api import create_app
    from verdict.scoring.model import FixedModel, ModelSource, StandInModel

    models: ModelSource = (
        FixedModel(StandInModel()) if flag is None else FlaggedModels(flag, _known_models())
    )
    decider = Decider(features=EngineFeatures(FeatureEngine()), models=models)
    uvicorn.run(create_app(decider), host=host, port=port, log_level="warning")


if __name__ == "__main__":  # pragma: no cover
    app()
