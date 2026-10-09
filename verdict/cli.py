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
from verdict.events.schema import DecisionEvent, SchemaFingerprint, TransactionEvent
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

    This is the operation run when the secret is published, the day after
    the live window, and the one a stranger runs to check that the live drift
    results were not arranged after the fact.

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
    history: Annotated[
        bool, typer.Option(help="Stage every decision to a temporary spool, as live (ADR 18).")
    ] = False,
    compression: Annotated[
        str, typer.Option(help="The scorer's producer compression on redpanda: none or zstd.")
    ] = "none",
    model: Annotated[
        str | None, typer.Option(help="Decide with the shipped model in this role (champion).")
    ] = None,
    shadow: Annotated[
        str | None, typer.Option(help="Score the shipped model in this role in shadow.")
    ] = None,
    shadow_kept_only: Annotated[
        bool, typer.Option(help="Shadow only what history could keep, as live (ADR 11).")
    ] = False,
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
        history: Whether to stage decisions, as the live scorer does.
        compression: Producer compression for the scorer's writes.
        model: The shipped model to decide with, by role; the stand-in if none.
        shadow: The shipped model to score in shadow, by role.
        shadow_kept_only: Shadow only the decisions history could keep.

    Raises:
        typer.BadParameter: If the stream is not one this build knows.
    """
    import platform
    import tempfile
    from dataclasses import asdict

    from verdict.scoring import loadtest as load

    match stream:
        case "memory":
            backend = load.memory_backend()
        case "redpanda":
            backend = load.redpanda_backend(bootstrap, compression=compression)
        case _:
            msg = f"unknown stream {stream!r}; use memory or redpanda"
            raise typer.BadParameter(msg)

    from verdict.scoring.registry import version_of

    known = _known_models()
    deciding = None if model is None else known[version_of(model)]
    shadowing = None if shadow is None else known[version_of(shadow)]
    shadow_when = None
    if shadow_kept_only:
        from verdict.history.sampling import SampleRates, could_be_kept

        rates = SampleRates()

        def shadow_when(event: TransactionEvent, decision: DecisionEvent) -> bool:
            return could_be_kept(event.event_id, decision.action, rates)

    sent = load.generate_events(events, rate=rate)
    with tempfile.TemporaryDirectory(prefix="verdict-history-") as staging:
        spool_root = Path(staging) if history else None
        results = [
            load.run_once(
                backend,
                sent,
                rate=rate,
                warmup=warmup,
                history=spool_root,
                model=deciding,
                shadow=shadowing,
                shadow_when=shadow_when,
            )
            for _ in range(runs)
        ]
    report = {
        "track": "synthetic live, local",
        "stream": backend.name,
        "model": "stand-in-0 (not trained; see verdict/scoring/model.py)"
        if deciding is None
        else deciding.version,
        "shadow": None if shadowing is None else shadowing.version,
        "shadow_kept_only": shadow_kept_only,
        "history_staged": history,
        "compression": compression if stream == "redpanda" else None,
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
    history: Annotated[
        Path | None,
        typer.Option(help="Stage every decision with its features under this history root."),
    ] = None,
    compression: Annotated[
        str, typer.Option(help="Producer compression for decisions: none or zstd (ADR 18).")
    ] = "none",
    initial_champion: Annotated[
        str | None,
        typer.Option(
            help="With --flag: if the pointer does not exist yet, point it at the shipped "
            "model in this role (champion). Never moves an existing pointer."
        ),
    ] = None,
    shadow: Annotated[
        str | None,
        typer.Option(help="Score the shipped model in this role (challenger) in shadow."),
    ] = None,
    shadow_kept_only: Annotated[
        bool,
        typer.Option(
            help="Score in shadow only the decisions history could keep, which is all the "
            "promotion gate reads (ADR 11's addendum of 2026-09-27)."
        ),
    ] = False,
    engine_snapshot: Annotated[
        Path | None,
        typer.Option(help="Save the feature state here as it runs, and start from it (ADR 27)."),
    ] = None,
    snapshot_every: Annotated[
        float, typer.Option(help="Seconds between the starts of saves of the feature state.")
    ] = 900.0,
) -> None:
    """Run the stream scorer until interrupted: the platform's decision path.

    Consumes `transactions` from the local broker, writes `decisions`, sets
    aside what it cannot decide on `dead-letter`, and checkpoints after every
    durable batch (ADR 8). Ctrl+C, or SIGTERM where the platform delivers it,
    stops it after the batch in hand. Metrics listen on localhost by default:
    nothing about the scoring path is meant to be public (`PLAN.md` section 8).

    With `--engine-snapshot`, the feature state is saved a slice at a time
    between batches, and a scorer that finds a save starts from it and replays
    the records after it (ADR 27). Without one, or when a save cannot be used,
    it starts cold: every card "no history" until its windows refill.

    Args:
        bootstrap: Broker address. Defaults to the compose stack's.
        group: The consumer group.
        flag: A champion pointer to follow.
        metrics_port: Where to serve metrics, or 0 for nowhere.
        metrics_host: The interface metrics listen on.
        history: The history root (ADR 18), if decisions are to be staged.
        compression: Producer batch compression.
        initial_champion: The role a new pointer starts at.
        shadow: The role to score in shadow (ADR 11).
        shadow_kept_only: Spare the shadow the rows history will drop.
        engine_snapshot: Where the feature state is saved, if anywhere.
        snapshot_every: Seconds between the starts of saves.
    """
    import signal
    import threading

    from prometheus_client import start_http_server

    from verdict.features.engine import FeatureEngine
    from verdict.observe.metrics import ScorerMetrics
    from verdict.scoring import recovery, service
    from verdict.scoring.consumer import TRANSACTIONS_TOPIC, StreamScorer
    from verdict.scoring.core import Decider, EngineFeatures
    from verdict.scoring.flags import FlaggedModels, set_champion
    from verdict.scoring.model import FixedModel, ModelSource, StandInModel
    from verdict.scoring.registry import version_of
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    known = _known_models()
    if flag is not None and initial_champion is not None and not flag.exists():
        set_champion(flag, version_of(initial_champion), known)
    models: ModelSource = FixedModel(StandInModel()) if flag is None else FlaggedModels(flag, known)
    shadow_models: ModelSource | None = (
        None if shadow is None else FixedModel(known[version_of(shadow)])
    )
    metrics = ScorerMetrics()
    stream = RedpandaStream(bootstrap or DEFAULT_BOOTSTRAP, compression=compression)
    staging = None
    if history is not None:
        from verdict.history import spool
        from verdict.history.compact import HistoryPaths
        from verdict.history.records import staged_schema

        paths = HistoryPaths(history)
        spool.recover(paths.staged)
        staging = spool.SpoolWriter(paths.staged, staged_schema())
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    engine = FeatureEngine()
    restored = None
    if engine_snapshot is not None:
        outcome = recovery.restore(engine_snapshot, stream, group=group, topic=TRANSACTIONS_TOPIC)
        if isinstance(outcome, recovery.Restored):
            restored, engine = outcome, outcome.engine
            metrics.restored.set(1)
            metrics.restore_seconds.set(outcome.seconds)
            metrics.replayed.set(outcome.replayed)
            typer.echo(
                f"restored {outcome.entities:,} entities saved from "
                f"{outcome.snapshot_started_at.isoformat(timespec='seconds')} and replayed "
                f"{outcome.replayed:,} records in {outcome.seconds:.1f}s"
            )
        else:
            metrics.restored.set(0)
            typer.echo(f"starting with empty feature windows: {outcome.reason}")
        from verdict.drift.live import STARTS_FILE, record_start

        # The drift monitors leave a day unjudged if the scorer started cold
        # within a day of it (ADR 28), so every start is written down.
        record_start(
            engine_snapshot / STARTS_FILE,
            at=dt.datetime.now(dt.UTC),
            restored=restored is not None,
            detail=f"restored {outcome.entities} entities, replayed {outcome.replayed}"
            if isinstance(outcome, recovery.Restored)
            else outcome.reason,
        )
    shadow_when = None
    if shadow_kept_only:
        from verdict.history.sampling import SampleRates, could_be_kept

        rates = SampleRates()

        def shadow_when(event: TransactionEvent, decision: DecisionEvent) -> bool:
            return could_be_kept(event.event_id, decision.action, rates)

    decider = Decider(
        features=EngineFeatures(engine),
        models=models,
        shadow=shadow_models,
        shadow_when=shadow_when,
    )
    scorer = StreamScorer(
        stream,
        decider=decider,
        group=group,
        on_decided=metrics.on_decided,
        history=staging,
    )
    snapshots = None
    if engine_snapshot is not None:
        if restored is not None:
            for event_id in restored.ledger:
                decider.remember(event_id)
            scorer.recent.extend(restored.recent)
            scorer.before_recent = restored.before_recent
            scorer.marks.clear()
            scorer.marks.extend(restored.marks)
        else:
            scorer.before_recent = stream.committed(TRANSACTIONS_TOPIC, group, "0")
            if scorer.before_recent is not None:
                # Started mid-partition with nothing restored: the stream before
                # here is unknown, so a save waits for an hour of it (ADR 27).
                scorer.marks.clear()
        snapshots = recovery.Snapshotter(
            engine_snapshot,
            scorer,
            engine,
            topic=TRANSACTIONS_TOPIC,
            every_seconds=snapshot_every,
            on_saved=metrics.on_saved,
        )
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
        summary = service.run(scorer, stop=stop, metrics=metrics, snapshots=snapshots)
    finally:
        if staging is not None:
            staging.close()
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


history_app = typer.Typer(
    name="history", help="What the platform keeps: ADR 18's sample.", no_args_is_help=True
)
app.add_typer(history_app)


@history_app.command("labels")
def history_labels(
    root: Annotated[Path, typer.Option(help="The history root.")],
    bootstrap: Annotated[str | None, typer.Option(help="Broker address.")] = None,
    metrics_port: Annotated[
        int, typer.Option(help="Serve Prometheus metrics on this port; 0 for none.")
    ] = 0,
    metrics_host: Annotated[str, typer.Option(help="Interface the metrics listen on.")] = (
        "127.0.0.1"
    ),
) -> None:
    """Collect labels into the label spool until interrupted.

    Args:
        root: The history root.
        bootstrap: Broker address. Defaults to the compose stack's.
        metrics_port: Where to serve metrics, or 0 for nowhere.
        metrics_host: The interface metrics listen on.
    """
    import signal
    import threading

    from prometheus_client import start_http_server

    from verdict.history import spool
    from verdict.history.compact import HistoryPaths
    from verdict.history.labels import CollectorMetrics, LabelCollector
    from verdict.history.records import LABEL_SCHEMA
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    paths = HistoryPaths(root)
    spool.recover(paths.labels)
    writer = spool.SpoolWriter(paths.labels, LABEL_SCHEMA)
    stream = RedpandaStream(bootstrap or DEFAULT_BOOTSTRAP)
    metrics = CollectorMetrics()
    collector = LabelCollector(stream, writer, metrics=metrics)
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    stop = threading.Event()

    def ask_to_stop(signum: int, frame: object) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGINT, ask_to_stop)
    signal.signal(signal.SIGTERM, ask_to_stop)
    try:
        while not stop.is_set():
            collector.poll()
    finally:
        writer.close()
        stream.close()
    typer.echo(
        f"stopped: {collector.stats.written} labels written, "
        f"{collector.stats.unreadable} unreadable"
    )


@history_app.command("compact")
def history_compact(
    root: Annotated[Path, typer.Option(help="The history root.")],
    seal_limit: Annotated[
        int,
        typer.Option(
            help="Seal at most this many hours per run, so a backlog is worked off a "
            "few hours at a time rather than in one process (docs/STATE.md, the "
            "dry run's first night)."
        ),
    ] = 4,
    seal: Annotated[bool, typer.Option(help="Seal finished hours.")] = True,
    finalise: Annotated[
        bool, typer.Option(help="Finalise every day that is ready, after any sealing.")
    ] = True,
) -> None:
    """Seal finished hours and finalise every day whose labels are all in.

    Run often; sealing is cheap once there is no backlog. Prints each day's
    manifest as it is finalised. The compactor runs the two steps as
    separate processes (`--no-finalise`, `--no-seal`), so a slow finalise
    never holds sealing back (ADR 18's second addendum); by hand, both run.

    Args:
        root: The history root.
        seal_limit: The most hours to seal in this run.
        seal: Whether to seal.
        finalise: Whether to finalise.
    """
    from dataclasses import asdict

    from verdict.history.compact import HistoryPaths, finalisable, finalise_day, seal_closed

    paths = HistoryPaths(root)
    now = dt.datetime.now(dt.UTC)
    if seal:
        for name in seal_closed(paths, now, limit=seal_limit):
            typer.echo(f"sealed {name}")
    if finalise:
        for day in finalisable(paths, now):
            manifest = finalise_day(paths, day, as_of=now)
            typer.echo(json.dumps(asdict(manifest), sort_keys=True))


@history_app.command("compactor")
def history_compactor(
    root: Annotated[Path, typer.Option(help="The history root.")],
    every: Annotated[
        float, typer.Option(help="Seconds between the end of one run of a step and the next.")
    ] = 300.0,
    seal_limit: Annotated[int, typer.Option(help="The most hours one run seals.")] = 4,
    seal_timeout: Annotated[
        float, typer.Option(help="Seconds a seal run may take before it is stopped.")
    ] = 1800.0,
    finalise_timeout: Annotated[
        float, typer.Option(help="Seconds a finalise run may take before it is stopped.")
    ] = 7200.0,
    metrics_port: Annotated[
        int, typer.Option(help="Serve Prometheus metrics on this port; 0 for none.")
    ] = 0,
    metrics_host: Annotated[str, typer.Option(help="Interface the metrics listen on.")] = (
        "127.0.0.1"
    ),
) -> None:
    """Seal and finalise every few minutes, each run in its own process, and report.

    Sealing and finalising are two loops side by side, so a finalise that is
    slow or stuck never holds sealing back, and each run has a time limit
    after which it is stopped and counted (ADR 18's second addendum). A run
    that exits gives its memory back before the next; this parent outlives
    the runs, counts how each ended, and reports how long the current one
    has taken, how old the oldest unsealed hour is and how many days wait
    to be finalised (`verdict/history/compactor.py`).

    Args:
        root: The history root.
        every: The wait between runs of a step.
        seal_limit: The most hours one run seals.
        seal_timeout: The time limit of a seal run.
        finalise_timeout: The time limit of a finalise run.
        metrics_port: Where to serve metrics, or 0 for nowhere.
        metrics_host: The interface metrics listen on.
    """
    import signal
    import threading

    from prometheus_client import start_http_server

    from verdict.history.compact import HistoryPaths
    from verdict.history.compactor import CompactorMetrics, Step, compact_command, run_forever

    limits = {Step.SEAL: seal_timeout, Step.FINALISE: finalise_timeout}
    metrics = CompactorMetrics(HistoryPaths(root), limits=limits)
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    stop = threading.Event()

    def ask_to_stop(signum: int, frame: object) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGINT, ask_to_stop)
    signal.signal(signal.SIGTERM, ask_to_stop)
    loops = [
        threading.Thread(
            target=run_forever,
            args=(step, compact_command(root, seal_limit, step), metrics),
            kwargs={"every_seconds": every, "timeout_seconds": limit, "stop": stop},
            name=f"compactor-{step.value}",
            daemon=True,
        )
        for step, limit in limits.items()
    ]
    for loop in loops:
        loop.start()
    # Signals reach only the main thread, so it waits here, interruptibly,
    # rather than in a join; each loop stops after the run it has in hand.
    while not stop.wait(1.0):
        pass
    for loop in loops:
        loop.join()


@history_app.command("quality")
def history_quality(
    root: Annotated[Path, typer.Option(help="The history root.")],
    out: Annotated[Path | None, typer.Option(help="Write the report here as JSON.")] = None,
) -> None:
    """How good the decisions were, from the finalised days' kept rows and labels.

    Weighted by each kept row's weight (ADR 18): the fraud share, each
    action's share, fraud rate and share of all fraud, and the champion's
    and shadow model's calibration by score band (`verdict/history/quality.py`).
    Reads only the kept days; writes nothing on the volume.

    Args:
        root: The history root.
        out: Where to write the report, or None to print it.
    """
    import pyarrow.parquet as pq

    from verdict.history.compact import HistoryPaths
    from verdict.history.quality import COLUMNS, report

    paths = HistoryPaths(root)
    days = [
        (path.stem, pq.read_table(path, columns=list(COLUMNS)))
        for path in sorted(paths.kept.glob("*.parquet"))
    ]
    if not days:
        typer.echo(f"no finalised days under {paths.kept}")
        raise typer.Exit(code=1)
    text = json.dumps(report(days), indent=2, sort_keys=True)
    if out is None:
        typer.echo(text)
    else:
        out.write_text(text + "\n", encoding="utf-8")
        typer.echo(f"wrote {out} from {len(days)} days")


@history_app.command("footprint")
def history_footprint(
    work: Annotated[
        Path, typer.Option(help="Scratch space for the hours written; outside any synced folder.")
    ],
    rows: Annotated[
        list[int] | None, typer.Option(help="Staged rows in the hour; repeat for each size.")
    ] = None,
    out: Annotated[Path, typer.Option(help="Where to write the report.")] = Path(
        "docs/finalise-footprint.json"
    ),
) -> None:
    """Measure the memory finalising a day holds per staged hour (ADR 18's addendum).

    Writes and finalises an hour at each size in fresh processes. At the
    default sizes, under a minute and about 1.5 GB of memory at the peak.

    Args:
        work: Scratch space.
        rows: The sizes.
        out: Where to write the report.
    """
    from verdict.history.footprint import measure

    report = measure(rows or [500_000, 1_000_000, 2_000_000], work)
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    typer.echo(
        f"{report['mb_per_million_rows']} MB per million staged rows; a live hour is about "
        f"{report['live_hour_estimate_mb']} MB"
    )


live_app = typer.Typer(
    name="live", help="The live feeds: the generator in real time (ADR 15).", no_args_is_help=True
)
app.add_typer(live_app)


def _read_if_present(path: Path | None, *, strip: bool = False) -> str | None:
    """A file's text, or None if there is no such file.

    The live stack always passes the schedule's files; on the development
    schedule they do not exist, and only `sealed` requires them.
    """
    if path is None or not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    return text.strip() if strip else text


def _run_feed(
    feed_name: str,
    *,
    state: Path,
    start: str,
    rate: float,
    schedule: str,
    bootstrap: str | None,
    metrics_port: int,
    metrics_host: str,
    from_start: bool,
    secret_file: Path | None,
    commitment_file: Path | None,
) -> None:
    import signal
    import threading

    from prometheus_client import start_http_server

    from verdict.live.feed import (
        Feed,
        FeedMetrics,
        LiveFeed,
        SnapshotStore,
        live_schedule,
        open_run,
    )
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    feed = Feed(feed_name)
    window_start = dt.datetime.fromisoformat(start)
    if window_start.tzinfo is None:
        typer.echo("--start must carry a time zone, for example 2026-10-01T00:00:00+00:00")
        raise typer.Exit(code=2)
    from verdict.events.generator.driver import LIVE_DAILY_CYCLE

    config = GeneratorConfig(
        events_per_second=rate,
        start_time=window_start.astimezone(dt.UTC),
        # Traffic rises and falls through the day as card traffic does
        # (750 to 1,250 a second around 1,000); both feeds play the same stream.
        daily_cycle=LIVE_DAILY_CYCLE,
        schedule=live_schedule(
            schedule,
            secret=_read_if_present(secret_file, strip=True),
            commitment=_read_if_present(commitment_file),
        ),
    )
    store = SnapshotStore(state, feed)
    run = open_run(config, store, now=dt.datetime.now(dt.UTC), from_start=from_start)
    metrics = FeedMetrics(feed)
    stream = RedpandaStream(bootstrap or DEFAULT_BOOTSTRAP)
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    stop = threading.Event()

    def ask_to_stop(signum: int, frame: object) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGINT, ask_to_stop)
    signal.signal(signal.SIGTERM, ask_to_stop)
    typer.echo(
        f"{feed.value} feed from {config.start_time.isoformat()} at {rate:g}/s on the "
        f"{schedule} schedule, {run.emitted:,} records already sent"
    )
    try:
        LiveFeed(run, stream, feed, store, metrics=metrics, save_in_background=True).run_until(
            stop.is_set
        )
    finally:
        stream.close()
    typer.echo(f"stopped at {run.emitted:,} records; place saved in {store.path}")


_STATE = Annotated[Path, typer.Option(help="Where the feed saves its place (the data volume).")]
_START = Annotated[str, typer.Option(help="The window's start, ISO 8601 with a zone.")]
_RATE = Annotated[float, typer.Option(help="Legitimate transactions a second.")]
_SCHEDULE = Annotated[
    str,
    typer.Option(help="dev, or sealed (needs --secret-file and --commitment-file)."),
]
_BOOTSTRAP = Annotated[str | None, typer.Option(help="Broker address.")]
_METRICS_HOST = Annotated[str, typer.Option(help="Interface the metrics listen on.")]
_SECRET_FILE = Annotated[
    Path | None, typer.Option(help="A file holding the sealed secret; never printed.")
]
_COMMITMENT_FILE = Annotated[
    Path | None, typer.Option(help="The committed hashes, docs/sealed-schedule.json.")
]
_FROM_START = Annotated[
    bool, typer.Option(help="Start from the window's start even with no saved place.")
]


@live_app.command("transactions")
def live_transactions(
    state: _STATE,
    start: _START,
    rate: _RATE = 1000.0,
    schedule: _SCHEDULE = "dev",
    bootstrap: _BOOTSTRAP = None,
    metrics_port: Annotated[int, typer.Option(help="Metrics port; 0 for none.")] = 9109,
    metrics_host: _METRICS_HOST = "127.0.0.1",
    from_start: _FROM_START = False,
    secret_file: _SECRET_FILE = None,
    commitment_file: _COMMITMENT_FILE = None,
) -> None:
    """Send each transaction to `transactions` when its event time comes.

    Args:
        state: Where the feed saves its place.
        start: The window's start.
        rate: Legitimate transactions a second.
        schedule: dev or sealed.
        bootstrap: Broker address.
        metrics_port: Metrics port.
        metrics_host: Metrics interface.
        from_start: Start fresh even well into the window.
        secret_file: The sealed secret's file, for `sealed`.
        commitment_file: The committed hashes, for `sealed`.
    """
    _run_feed(
        "transactions",
        state=state,
        start=start,
        rate=rate,
        schedule=schedule,
        bootstrap=bootstrap,
        metrics_port=metrics_port,
        metrics_host=metrics_host,
        from_start=from_start,
        secret_file=secret_file,
        commitment_file=commitment_file,
    )


@live_app.command("labels")
def live_labels(
    state: _STATE,
    start: _START,
    rate: _RATE = 1000.0,
    schedule: _SCHEDULE = "dev",
    bootstrap: _BOOTSTRAP = None,
    metrics_port: Annotated[int, typer.Option(help="Metrics port; 0 for none.")] = 9110,
    metrics_host: _METRICS_HOST = "127.0.0.1",
    from_start: _FROM_START = False,
    secret_file: _SECRET_FILE = None,
    commitment_file: _COMMITMENT_FILE = None,
) -> None:
    """Send each label to `labels` when its label time comes, a week later.

    Runs the same stream as the transaction feed, from the same start, and
    must be given the same configuration: its snapshot refuses any other.

    Args:
        state: Where the feed saves its place.
        start: The window's start.
        rate: Legitimate transactions a second.
        schedule: dev or sealed.
        bootstrap: Broker address.
        metrics_port: Metrics port.
        metrics_host: Metrics interface.
        from_start: Start fresh even well into the window.
        secret_file: The sealed secret's file, for `sealed`.
        commitment_file: The committed hashes, for `sealed`.
    """
    _run_feed(
        "labels",
        state=state,
        start=start,
        rate=rate,
        schedule=schedule,
        bootstrap=bootstrap,
        metrics_port=metrics_port,
        metrics_host=metrics_host,
        from_start=from_start,
        secret_file=secret_file,
        commitment_file=commitment_file,
    )


@app.command(name="engine-footprint")
def engine_footprint(
    events: Annotated[int, typer.Option(help="The most events to serve.")] = 800_000,
    out: Annotated[Path, typer.Option(help="Where to write the report.")] = Path(
        "docs/engine-footprint.json"
    ),
    steady: Annotated[
        bool,
        typer.Option(help="Measure the steady state instead: 26 scaled hours, times fifty."),
    ] = False,
) -> None:
    """Measure the feature engine's memory as events pass through it (ADR 15, ADR 20).

    CPU and memory heavy: a few minutes either way.

    Args:
        events: The most events to serve, without --steady.
        out: Where to write the report.
        steady: Run the scaled configuration past a full day and project to live.
    """
    from dataclasses import asdict

    from verdict.features.footprint import measure, steady_state

    if steady:
        result = steady_state()
        out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        typer.echo(f"about {result['live_estimate_gb']} GB at the live rate, steady")
        return

    report = measure(events)
    out.write_text(json.dumps(asdict(report), indent=2) + "\n", encoding="utf-8")
    typer.echo(
        f"{report.bytes_per_entity:,.0f} bytes an entity, {report.bytes_per_event:,.0f} an "
        f"event held; a day at the live rate is about {report.day_estimate_gb:,.1f} GB"
    )


train_app = typer.Typer(
    name="train", help="Week 5: the champion, the challenger, the leak.", no_args_is_help=True
)
app.add_typer(train_app)

_TRACK = Annotated[str, typer.Option(help="real (offline, the competition data) or synthetic.")]
_WORK = Annotated[
    Path,
    typer.Option(help="Where tables and models go; outside git and outside any synced folder."),
]


def _work(work: Path, track: str) -> Path:
    folder = work / track
    folder.mkdir(parents=True, exist_ok=True)
    return folder


@train_app.command("replay")
def train_replay(
    track: _TRACK,
    work: _WORK,
    days: Annotated[float, typer.Option(help="Stream time, for synthetic.")] = 10.0,
    unfixed: Annotated[
        bool, typer.Option(help="Serve from the unfixed engine, for the leak measurement.")
    ] = False,
) -> None:
    """Replay a track through the engine into a training table.

    Args:
        track: real or synthetic.
        work: The working directory.
        days: Stream time, for synthetic.
        unfixed: Use the engine with the same-instant leak.
    """
    import os

    from verdict.features.unfixed import ObserveImmediatelyEngine
    from verdict.models.champion import replay

    source = os.environ.get("VERDICT_IEEE_CIS_DIR")
    report = replay(
        track,
        _work(work, track) / ("leaky.parquet" if unfixed else "fixed.parquet"),
        source=Path(source) if source else None,
        days=days,
        engine=ObserveImmediatelyEngine() if unfixed else None,
    )
    typer.echo(
        f"served {report.served:,}, kept {report.kept:,} ({report.frauds:,} frauds) "
        f"into {report.path}"
    )


@train_app.command("champion")
def train_champion(
    track: _TRACK,
    work: _WORK,
    report: Annotated[Path, typer.Option(help="Where the report goes.")],
) -> None:
    """Fit, test, export and time the champion on a replayed track.

    Args:
        track: real or synthetic.
        work: The working directory holding the track's table.
        report: The JSON report.
    """
    from verdict.models.champion import train_track

    folder = _work(work, track)
    result = train_track(track, folder / "fixed.parquet", folder / "champion.onnx")
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    tested = result["test_pr_auc"]
    typer.echo(
        f"{result['model']}: test PR-AUC {tested['value']:.4f} "
        f"({tested['low']:.4f} to {tested['high']:.4f}), {result['track']}"
    )


@train_app.command("challenger")
def train_challenger(
    track: _TRACK,
    work: _WORK,
    report: Annotated[Path, typer.Option(help="Where the report goes.")],
) -> None:
    """Fit the FT-Transformer on the champion's split and compare them, paired.

    Args:
        track: real or synthetic.
        work: The working directory holding the table and the champion.
        report: The JSON report.
    """
    from verdict.models.champion import challenge_track

    folder = _work(work, track)
    result = challenge_track(
        track, folder / "fixed.parquet", folder / "champion.onnx", folder / "challenger.onnx"
    )
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    diff = result["challenger_minus_champion"]
    typer.echo(
        f"challenger minus champion: {diff['value']:+.4f} ({diff['low']:+.4f} to "
        f"{diff['high']:+.4f}), {result['track']}"
    )


@train_app.command("compare")
def train_compare(
    track: _TRACK,
    work: _WORK,
    report: Annotated[Path, typer.Option(help="The challenger report to bring up to date.")],
) -> None:
    """Recompute the paired comparison after one of the two models is rebuilt.

    Refitting the champion does not need the challenger refitted with it: both
    are exported, so the comparison can be scored again from the files. The
    challenger's fit block is kept, because that fit is the one being reported.

    Args:
        track: real or synthetic.
        work: The working directory holding the table and both models.
        report: The existing challenger report, rewritten in place.
    """
    from verdict.models.champion import compare_models

    folder = _work(work, track)
    result = compare_models(
        track, folder / "fixed.parquet", folder / "champion.onnx", folder / "challenger.onnx"
    )
    previous = json.loads(report.read_text(encoding="utf-8")) if report.exists() else {}
    if "fit" in previous:
        result["fit"] = previous["fit"]
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    diff = result["challenger_minus_champion"]
    typer.echo(
        f"challenger minus champion: {diff['value']:+.4f} ({diff['low']:+.4f} to "
        f"{diff['high']:+.4f}), {result['track']}"
    )


@train_app.command("leak")
def train_leak(
    work: _WORK,
    report: Annotated[Path, typer.Option(help="Where the report goes.")],
) -> None:
    """What the same-instant leak would have added to the real track's PR-AUC.

    Needs `train replay --track real` run twice, with and without `--unfixed`.

    Args:
        work: The working directory.
        report: The JSON report.
    """
    from verdict.models.champion import leak_inflation

    folder = _work(work, "real")
    result = leak_inflation(folder / "fixed.parquet", folder / "leaky.parquet")
    report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    diff = result["inflation"]
    typer.echo(
        f"leaky minus fixed PR-AUC: {diff['value']:+.4f} ({diff['low']:+.4f} to "
        f"{diff['high']:+.4f}); {result['test_rows_whose_features_differ']} test rows differ"
    )


@app.command(name="retrain")
def retrain_command(
    track: _TRACK,
    work: _WORK,
    drift: Annotated[Path, typer.Option(help="The drift report whose request asks for this.")],
    report: Annotated[Path, typer.Option(help="Where the JSON report goes.")] = Path(
        "docs/retrain.json"
    ),
    pull_request: Annotated[Path, typer.Option(help="Where the pull request body goes.")] = Path(
        "docs/retrain-pull-request.md"
    ),
    train_share: Annotated[
        float, typer.Option(help="Where in the history the split falls; later is a later retrain.")
    ] = 0.7,
) -> None:
    """Fit the candidate a drift request asks for, and write its pull request.

    This is the end of the automated path. It does not move the champion
    pointer and does not promote anything: the pull request it writes is for
    a person to read, and a second one carrying the promotion gate's verdict
    on a labelled shadow window is what moves the pointer (ADR 11).

    Args:
        track: real or synthetic.
        work: The working directory holding the table and the champion.
        drift: The drift report whose first request asks for the retrain.
        report: Where the JSON report goes.
        pull_request: Where the pull request body goes.
        train_share: Where in the history the split falls.
    """
    from verdict.models.retrain import (
        pull_request_body,
        request_from_report,
        retrain,
        write_pull_request,
    )
    from verdict.review_queue.evaluate import write_report

    folder = _work(work, track)
    request = request_from_report(json.loads(drift.read_text(encoding="utf-8")))
    result = retrain(
        request,
        track=track,
        table_path=folder / "fixed.parquet",
        champion_path=folder / "champion.onnx",
        candidate_path=folder / "candidate.onnx",
        train_share=train_share,
    )
    write_report(result, report)
    write_pull_request(pull_request_body(result, request), pull_request)
    difference = result["comparison"]["challenger_minus_champion"]
    coverage = result["drifted_days_in_training"]
    typer.echo(
        f"candidate {result['comparison']['challenger']}: "
        f"{difference['value']:+.4f} ({difference['low']:+.4f} to {difference['high']:+.4f}) "
        f"against the champion; "
        + ("trained past the drift" if coverage["covers_the_drift"] else "has not seen the drift")
        + f"; pull request written to {pull_request}, nothing promoted"
    )


@app.command(name="drift-report")
def drift_report(
    days: Annotated[float, typer.Option(help="Days of synthetic stream to replay.")] = 50.0,
    after_days: Annotated[
        float, typer.Option(help="The champion's cutoff: before it, the reference.")
    ] = 7.0,
    report: Annotated[Path, typer.Option(help="Where the report goes.")] = Path(
        "docs/drift-report.json"
    ),
    rate: Annotated[float, typer.Option(help="Share of each day judged, by hash draw.")] = 0.03,
) -> None:
    """Run the drift monitors over a replayed stream and report what fires.

    The days before `after_days` build the fixed reference, which is the
    champion's training window; every day after is judged against it, and the
    trigger is asked once per day as the scheduled job would ask it. The
    development schedule's regime days are reported beside the firings so the
    delay can be read (ADR 12).

    Args:
        days: Days of synthetic stream to replay.
        after_days: The champion's cutoff, in days from the stream's start.
        report: Where the JSON report goes.
        rate: Share of each day's transactions the monitors judge on.
    """
    from verdict.drift.monitors import DailyReport
    from verdict.drift.run import run_report
    from verdict.models.champion import SYNTHETIC, synthetic_records
    from verdict.review_queue.evaluate import write_report
    from verdict.scoring.onnx_model import OnnxModel
    from verdict.scoring.registry import ARTIFACTS

    def say(judged: DailyReport) -> None:
        drifted = ", ".join(sorted(judged.drifted())) or "none"
        typer.echo(f"{judged.day.isoformat()}: {drifted}", err=True)

    result = run_report(
        synthetic_records(days),
        model=OnnxModel(ARTIFACTS / "champion.onnx"),
        cutoff=SYNTHETIC.start_time + dt.timedelta(days=after_days),
        schedule=SYNTHETIC.schedule,
        start_time=SYNTHETIC.start_time,
        rate=rate,
        on_day=say,
    )
    write_report(result, report)
    opened = result["first_request"]
    drifted_days = sum(1 for day in result["days"] if day["drifted"])
    typer.echo(
        f"{result['days_judged']} days judged, {drifted_days} with drift; "
        + (
            "no retraining request opened"
            if opened is None
            else f"first request on {opened['opened_on']} "
            f"after {opened['after_days_judged']} days judged, on "
            f"{', '.join(opened['quantities'])}"
        )
    )


@app.command(name="drift-cycle-check")
def drift_cycle_check(
    reference: Annotated[Path, typer.Option(help="The drift reference to judge against.")],
    out: Annotated[Path, typer.Option(help="Where the JSON report goes.")],
    cycle: Annotated[bool, typer.Option(help="Replay with the live daily cycle, or flat.")] = True,
    seed: Annotated[int, typer.Option(help="A seed other than the reference's.")] = 20270202,
    days: Annotated[float, typer.Option(help="Days of stream to replay.")] = 6.0,
    judge_from_day: Annotated[int, typer.Option(help="The first day judged.")] = 2,
) -> None:
    """Judge a fresh synthetic stream against a kept drift reference (ADR 29).

    Replays the champion's scaled training configuration under another seed,
    with or without the live daily cycle, and reports what the monitors say
    about each day from `judge_from_day` on. Every day is before the first
    regime change, so a quantity flagged here is flagged for the traffic's
    shape, not for fraud.

    Args:
        reference: The reference, as the models job keeps it.
        out: Where the JSON report goes.
        cycle: Whether the stream follows the live daily cycle.
        seed: The stream's seed.
        days: Days replayed.
        judge_from_day: The first day judged.
    """
    from dataclasses import replace

    from verdict.drift import live as drift_live
    from verdict.drift.monitors import DailyReport
    from verdict.drift.run import _rendered, judge_against
    from verdict.events.generator.driver import LIVE_DAILY_CYCLE
    from verdict.models.champion import SYNTHETIC, synthetic_records
    from verdict.review_queue.evaluate import write_report
    from verdict.scoring.onnx_model import OnnxModel
    from verdict.scoring.registry import ARTIFACTS

    kept, meta = drift_live.load_reference(reference)
    config = replace(SYNTHETIC, seed=seed, daily_cycle=LIVE_DAILY_CYCLE if cycle else None)

    def say(judged: DailyReport) -> None:
        drifted = ", ".join(sorted(judged.drifted())) or "none"
        typer.echo(f"{judged.day.isoformat()}: {drifted}", err=True)

    reports = judge_against(
        synthetic_records(days, config),
        kept,
        model=OnnxModel(ARTIFACTS / "champion.onnx"),
        judge_from=SYNTHETIC.start_time + dt.timedelta(days=judge_from_day),
        on_day=say,
    )
    write_report(
        {
            "track": "synthetic, offline replay",
            "reference": meta,
            "stream": {
                "seed": seed,
                "daily_cycle": None if not cycle else repr(LIVE_DAILY_CYCLE),
                "days": days,
                "judged_from_day": judge_from_day,
            },
            "days": [_rendered(report) for report in reports],
            "drifted": {report.day.isoformat(): sorted(report.drifted()) for report in reports},
        },
        out,
    )
    flagged = sum(1 for report in reports if report.drifted())
    typer.echo(f"{len(reports)} days judged, {flagged} with a quantity flagged")


@app.command(name="queue-eval")
def queue_eval(
    days: Annotated[float, typer.Option(help="Days of synthetic stream to replay.")] = 20.0,
    after_days: Annotated[
        float, typer.Option(help="Collect only from this day on: the champion's cutoff.")
    ] = 7.0,
    report: Annotated[Path, typer.Option(help="Where the report goes.")] = Path(
        "docs/queue-eval.json"
    ),
    analysts: Annotated[int, typer.Option(help="Analysts on shift.")] = 8,
    reviews_per_hour: Annotated[
        int, typer.Option(help="Reviews one analyst completes in an hour.")
    ] = 12,
) -> None:
    """Measure expected-loss ranking of the review queue against score ranking.

    Replays the synthetic stream unsampled through the scorer's own engine,
    scores it with the shipped champion and decides it with the shipped
    rules, then runs both ranking policies over the same days (ADR 13).

    Everything is served, so the windows are what the scorer would have held,
    but the queue is collected only from `after_days`, because the champion
    is fitted on the first seven days of this same stream and a queue built
    from those would measure its memory rather than either policy.

    Args:
        days: Days of synthetic stream to replay.
        after_days: Collect only from this day of the stream on.
        report: Where the JSON report goes.
        analysts: Analysts on shift.
        reviews_per_hour: Reviews one analyst completes in an hour.
    """
    from verdict.models.champion import SYNTHETIC, synthetic_records
    from verdict.review_queue.evaluate import evaluate_queue, write_report
    from verdict.scoring.onnx_model import OnnxModel
    from verdict.scoring.registry import ARTIFACTS

    result = evaluate_queue(
        synthetic_records(days),
        model=OnnxModel(ARTIFACTS / "champion.onnx"),
        collect_after=SYNTHETIC.start_time + dt.timedelta(days=after_days),
        analysts=analysts,
        reviews_per_analyst_hour=reviews_per_hour,
    )
    write_report(result, report)
    caught = result["caught_per_analyst_hour_cents"]
    typer.echo(
        f"queue over {result['days']} days: {result['queued']:,} of "
        f"{result['scored_after_cutoff']:,} scored after the cutoff, "
        f"{result['queue_fraud_share']:.1%} fraud; expected loss minus score "
        f"${caught['difference'] / 100:,.2f} per analyst-hour "
        f"(${caught['low'] / 100:,.2f} to ${caught['high'] / 100:,.2f})"
    )


@app.command(name="models-request")
def models_request(
    state: Annotated[Path, typer.Option(help="The models job's state directory.")],
    reason: Annotated[str, typer.Option(help="Why a candidate is wanted, for its pull request.")],
) -> None:
    """Open a retraining request by hand, for a reason no drift monitor can see (ADR 32).

    The models job then fits a candidate as it would for drift, opens its pull
    request, and the candidate goes through shadow and the promotion gate. The
    request closes only when a candidate beats the incumbent. Refused while a
    request is already open.

    Args:
        state: The models job's state directory, as `models-job --state`.
        reason: Why, in a sentence or two; it opens the pull request.

    Raises:
        typer.Exit: With code 1 if a request is already open.
    """
    from verdict.drift import live as drift_live
    from verdict.drift.trigger import RetrainRequest

    drift_state = drift_live.DriftState(state / "drift")
    if drift_state.open_request() is not None:
        typer.echo("a request is already open; it is answered first")
        raise typer.Exit(code=1)
    now = dt.datetime.now(dt.UTC)
    drift_state.open(
        RetrainRequest(opened_on=now.date(), quantities=(), evidence=(), reason=reason.strip()),
        at=now,
    )
    typer.echo(f"request opened by hand on {now.date().isoformat()}; the next pass fits when due")


@app.command(name="models-job")
def models_job(
    history: Annotated[Path, typer.Option(help="The history root the scorer stages to.")],
    state: Annotated[Path, typer.Option(help="Where the job keeps its state.")],
    since: Annotated[str, typer.Option(help="The live window's start, RFC 3339.")],
    flag: Annotated[Path, typer.Option(help="The champion pointer.")],
    starts: Annotated[Path, typer.Option(help="The scorer's record of its starts.")],
    every: Annotated[float, typer.Option(help="Seconds between passes.")] = 3600.0,
    once: Annotated[bool, typer.Option(help="One pass, then stop.")] = False,
    metrics_port: Annotated[int, typer.Option(help="Serve metrics here; 0 for none.")] = 9113,
    metrics_host: Annotated[str, typer.Option(help="Interface the metrics listen on.")] = (
        "127.0.0.1"
    ),
) -> None:
    """The live window's drift monitors, retraining and promotion gate (ADR 28).

    Judges each finished day, fits a candidate when a drift request is open
    and one is due, and runs the gate on the shadow model's labelled week,
    opening a pull request for each; it never merges, deploys or moves the
    pointer. Pull requests need `VERDICT_GITHUB_TOKEN`; without it they are
    written under the state directory only, and the log says so.

    Args:
        history: The history root.
        state: The job's state directory.
        since: The live window's start.
        flag: The champion pointer.
        starts: The scorer's record of its starts.
        every: Seconds between passes.
        once: Run one pass and stop.
        metrics_port: Where metrics are served, or 0.
        metrics_host: The interface they listen on.
    """
    import os
    import time

    from prometheus_client import start_http_server

    from verdict.drift import live as drift_live
    from verdict.events.generator.regimes import MIN_REGIME_DAYS
    from verdict.history.compact import HistoryPaths
    from verdict.live.github import GitHub, urllib_transport
    from verdict.live.models_job import Job, JobMetrics, run_pass
    from verdict.scoring.flags import read_pointer
    from verdict.scoring.registry import path_of

    nice = getattr(os, "nice", None)
    if nice is not None:
        nice(19)
    metrics = JobMetrics()
    if metrics_port:
        start_http_server(metrics_port, addr=metrics_host, registry=metrics.registry)
    token = os.environ.get("VERDICT_GITHUB_TOKEN", "").strip()
    github = GitHub(urllib_transport(token)) if token else None
    if github is None:
        typer.echo("no VERDICT_GITHUB_TOKEN: pull requests are written locally only")
    window = dt.datetime.fromisoformat(since.replace("Z", "+00:00"))

    while True:
        try:
            pointer = read_pointer(flag)
            champion = pointer.champion
            champion_path = path_of(champion)
            # The reference is the live window's first two days on full
            # features, inside the first regime; after a promotion, the two
            # days after it (ADR 29).
            changed = (
                dt.datetime.fromisoformat(pointer.changed_at) if pointer.changed_at else window
            )
            promoted = changed > window
            ref_since = changed if promoted else window
            paths = HistoryPaths(history)
            reference_path = state / drift_live.REFERENCE_FILE
            reference, judge_from = None, None
            if reference_path.exists():
                kept, meta = drift_live.load_reference(reference_path)
                if meta.get("champion") == champion and meta.get("since") == ref_since.isoformat():
                    reference = kept
                    judge_from = dt.date.fromisoformat(meta["days"][-1]) + dt.timedelta(days=1)
            if reference is None:
                days = drift_live.reference_days(
                    paths,
                    ref_since,
                    drift_live.read_starts(starts),
                    dt.datetime.now(dt.UTC),
                    within=None if promoted else dt.timedelta(days=MIN_REGIME_DAYS),
                )
                if days is not None:
                    reference = drift_live.reference_from_history(paths, days)
                    drift_live.save_reference(
                        reference,
                        reference_path,
                        meta={
                            "champion": champion,
                            "since": ref_since.isoformat(),
                            "days": [day.isoformat() for day in days],
                        },
                    )
                    judge_from = days[-1] + dt.timedelta(days=1)
                    typer.echo(f"drift reference built from {days[0]} and {days[-1]}")
            job = Job(
                paths=paths,
                state_dir=state,
                reference=reference,
                judge_from=judge_from,
                since=window,
                starts_file=starts,
                champion_path=champion_path,
                shadow_path=None,
                github=github,
            )
            said = run_pass(job)
            metrics.passes.labels("ok").inc()
            metrics.pull_requests.labels("candidate").inc(1 if "candidate" in said else 0)
            metrics.pull_requests.labels("verdict").inc(1 if "verdict" in said else 0)
            metrics.days_judged.set(len(job.drift_state.judged()))
            metrics.request_open.set(1 if job.drift_state.open_request() else 0)
            typer.echo(json.dumps(said, default=str))
        except Exception as error:  # a pass that fails is counted and retried, not fatal
            metrics.passes.labels("failed").inc()
            typer.echo(f"pass failed: {type(error).__name__}: {error}", err=True)
        metrics.last_pass.set(time.time())
        if once:
            return
        time.sleep(every)


@app.command(name="rollback-drill")
def rollback_drill(
    champion: Annotated[Path, typer.Option(help="The ONNX champion rolled back from.")],
    runs: Annotated[int, typer.Option(help="Timed runs.")] = 5,
    out: Annotated[Path | None, typer.Option(help="Where the report goes.")] = None,
) -> None:
    """Time the rollback flag: flip it, and time the old champion's first decision.

    Rolls back from the ONNX champion to the stand-in, on the in-process
    stream at 1,000 transactions a second, `runs` times.

    Args:
        champion: The model rolled back from.
        runs: How many times.
        out: The JSON report.
    """
    import platform
    import tempfile
    from dataclasses import asdict

    from verdict.scoring.drill import run_drill
    from verdict.scoring.loadtest import generate_events
    from verdict.scoring.model import StandInModel
    from verdict.scoring.onnx_model import OnnxModel
    from verdict.scoring.timing import t_interval

    new = OnnxModel(champion)
    old = StandInModel()
    known: dict[str, Model] = {old.version: old, new.version: new}
    events = generate_events(6_000, rate=1_000.0)
    results = []
    with tempfile.TemporaryDirectory(prefix="verdict-drill-") as folder:
        for index in range(runs):
            results.append(
                run_drill(
                    Path(folder) / f"champion-{index}.json",
                    known,
                    old=old.version,
                    new=new.version,
                    events=events,
                )
            )
    seconds = [r.seconds_to_old_champion for r in results]
    interval = t_interval(seconds)
    mean, low, high = interval.mean, interval.low, interval.high
    report = {
        "track": "synthetic live, local",
        "rolled_back_from": new.version,
        "rolled_back_to": old.version,
        "rate_per_second": 1_000.0,
        "runs": [asdict(r) for r in results],
        "seconds_to_old_champion": {"mean": mean, "low": low, "high": high},
        "host": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        "measured_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
    }
    if out is not None:
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    typer.echo(
        f"old champion deciding {mean * 1000:.1f} ms after the flag "
        f"({low * 1000:.1f} to {high * 1000:.1f}), {runs} runs; "
        f"decisions by the rolled-back model after the flag: "
        f"{sum(r.decisions_by_new_after_rollback for r in results)}"
    )


chaos_app = typer.Typer(
    name="chaos",
    help="Faults done on purpose, for docs/failure-modes.md.",
    no_args_is_help=True,
)
app.add_typer(chaos_app)


@chaos_app.command("run")
def chaos_run(
    fault: Annotated[str, typer.Option(help="pause, throttle or scorer-stall.")],
    seconds: Annotated[float, typer.Option(help="How long the fault lasts.")],
    rate: Annotated[float, typer.Option(help="Transactions a second.")] = 1_000.0,
    count: Annotated[int, typer.Option(help="Transactions in all.")] = 90_000,
    container: Annotated[str, typer.Option(help="The broker's container.")] = "verdict-redpanda",
    mid_batch: Annotated[
        bool, typer.Option(help="Begin inside a scorer batch, before its flush.")
    ] = True,
    out: Annotated[Path | None, typer.Option(help="Write the report here as JSON.")] = None,
) -> None:
    """Do one fault while the scorer decides, and report what it did.

    Needs the local stack (deploy/compose) and Docker. The default is the
    live rate for a minute and a half: one core for the scorer, a little
    for the broker; nothing here is a timing.

    Args:
        fault: Which fault (verdict/chaos/faults.py).
        seconds: How long it lasts.
        rate: The send rate.
        count: How many to send.
        container: The broker's container.
        mid_batch: Begin it from inside a batch.
        out: Where to write the report.
    """
    from verdict.chaos.faults import as_dict, run_fault

    report = as_dict(
        run_fault(
            fault,
            seconds=seconds,
            rate=rate,
            count=count,
            container=container,
            mid_batch=mid_batch,
        )
    )
    text = json.dumps(report, indent=2)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    typer.echo(text)


observe_app = typer.Typer(
    name="observe", help="What the platform shows about itself.", no_args_is_help=True
)
app.add_typer(observe_app)


@observe_app.command("grafana")
def observe_grafana(
    out: Annotated[Path, typer.Option(help="Where to write Grafana's provisioning files.")],
    mounted_at: Annotated[
        str, typer.Option(help="Where Grafana sees that directory.")
    ] = "/etc/grafana/verdict",
) -> None:
    """Write the public dashboard and its data source for Grafana to load.

    Args:
        out: The directory to write.
        mounted_at: Its path inside the Grafana container.
    """
    from verdict.observe.dashboard import write_files

    for path in write_files(out, mounted_at=mounted_at):
        typer.echo(f"wrote {path}")


@observe_app.command("alerts")
def observe_alerts(
    outbox: Annotated[Path, typer.Option(help="Where messages go for the host to send.")] = Path(
        "/data/alerts/outbox"
    ),
    state: Annotated[Path, typer.Option(help="What has been told, across restarts.")] = Path(
        "/data/alerts/told.json"
    ),
    prometheus: Annotated[str, typer.Option(help="Prometheus base URL.")] = (
        "http://prometheus:9090"
    ),
    every: Annotated[float, typer.Option(help="Seconds between polls.")] = 60.0,
) -> None:
    """Relay Prometheus's firing alerts into the outbox, until interrupted (ADR 26).

    Args:
        outbox: Where messages are written.
        state: What has been told.
        prometheus: Prometheus's base URL.
        every: The wait between polls.
    """
    import signal
    import threading

    from verdict.observe.alerts import Relay, prometheus_reader, run_forever

    relay = Relay(outbox, state, read=prometheus_reader(prometheus))
    stop = threading.Event()

    def ask_to_stop(signum: int, frame: object) -> None:
        del signum, frame
        stop.set()

    signal.signal(signal.SIGINT, ask_to_stop)
    signal.signal(signal.SIGTERM, ask_to_stop)
    run_forever(relay, every_seconds=every, stop=stop)


@observe_app.command("report")
def observe_report(
    start: Annotated[str, typer.Option(help="First minute, ISO 8601 with a zone.")],
    end: Annotated[str, typer.Option(help="The minute after the last, ISO 8601 with a zone.")],
    prometheus: Annotated[str, typer.Option(help="Prometheus base URL.")] = (
        "http://prometheus:9090"
    ),
    interruptions: Annotated[
        Path, typer.Option(help="The spot notices the instances recorded.")
    ] = Path("/data/interruptions"),
    out: Annotated[Path | None, typer.Option(help="Write the report here as JSON.")] = None,
) -> None:
    """Latency while serving, and availability, over a stretch of the live stack.

    The two numbers of ADR 25: decision latency with each spot reclaim's
    recovery left out, on AWS's own notice as the evidence, and availability
    with every minute counted and each reclaim and each other stop listed.

    Args:
        start: The first minute.
        end: The minute after the last.
        prometheus: Where the live stack's metrics are.
        interruptions: The notices directory on the data volume.
        out: Where to write the full report.
    """
    from verdict.observe.availability import fetch_minutes, read_notices, report

    first = dt.datetime.fromisoformat(start).astimezone(dt.UTC)
    last = dt.datetime.fromisoformat(end).astimezone(dt.UTC)
    result = report(fetch_minutes(prometheus, first, last), read_notices(interruptions))
    if out is not None:
        out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    serving = result["latency_while_serving"]
    every = result["latency_every_minute"]
    availability = result["availability"]
    typer.echo(
        f"while serving, {serving['minutes']:,} minutes: p50 {serving['p50_ms']:.1f} ms, "
        f"p95 {serving['p95_ms']:.1f} ms, p99 {serving['p99_ms']:.1f} ms"
    )
    typer.echo(
        f"every minute: p99 {every['p99_ms']:.1f} ms; uptime "
        f"{availability['uptime_percent']:.3f} percent; "
        f"{len(availability['spot_reclaims'])} spot reclaims, "
        f"{len(availability['other_stops'])} other stops"
    )


flag_app = typer.Typer(
    name="flag", help="The champion pointer the scorer reads per event.", no_args_is_help=True
)
app.add_typer(flag_app)

DEFAULT_FLAG = Path("data/flags/champion.json")
"""Runtime state, not configuration: under `data/`, which git ignores."""


def _known_models() -> dict[str, Model]:
    """The models this build can score with, by version.

    The stand-in, and every ONNX model shipped in `verdict/models/artifacts`
    (`scoring/registry.py`).

    Returns:
        Version to model.
    """
    from verdict.scoring.registry import known_models

    return known_models()


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


@app.command(name="site-export")
def site_export(
    docs: Annotated[Path, typer.Option(help="The committed reports.")] = Path("docs"),
    out: Annotated[Path, typer.Option(help="The page's data file.")] = Path("site/results.json"),
) -> None:
    """Write the demo site's data from the committed reports (ADR 30).

    Args:
        docs: The directory of committed reports.
        out: The JSON file the page reads.
    """
    from verdict.publish.demo_site import export

    data = export(docs, out)
    typer.echo(f"wrote {out} from {len(data['sources'])} reports")


@app.command(name="site-serve")
def site_serve(
    root: Annotated[Path, typer.Option(help="The site directory.")] = Path("site"),
    port: Annotated[int, typer.Option(help="The local port.")] = 8080,
) -> None:
    """Serve the demo site locally with the headers its host sends (ADR 30).

    Args:
        root: The site directory.
        port: The local port.
    """
    from verdict.publish.demo_site import serve

    typer.echo(f"http://127.0.0.1:{port}/ (the host's headers, content security policy included)")
    serve(root, port)


if __name__ == "__main__":  # pragma: no cover
    app()
