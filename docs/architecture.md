# Architecture

Four figures, one per mechanism the README describes in prose: the platform as a whole,
one transaction's path through the scorer, how the published numbers are produced, and the
live stack. Each names the module that implements what it shows; the thirty-five decision
records in [adr/](adr/) say why each piece is the way it is.

## The platform

![Public competition data replayed offline and a synthetic generator played live both reach one stream consumer, which serves features computed once, scores with the champion while a challenger shadows it, and keeps every decision for the queue, the drift monitors and the dashboard.](architecture/system.svg)

Two tracks feed one platform. On the left, the IEEE-CIS competition data is mapped onto the
platform's own event schema (`verdict.events.ieee_cis_events`) and replayed in time order
into the feature engine (`verdict.features.engine`), which serves each event its features
before observing it and writes every value once to both halves of the Feast store
(`verdict.store`). In the middle, the synthetic generator (`verdict.events.generator`) plays a
sealed regime schedule through the live feeds (`verdict.live.feed`), transactions now and
labels a week later, onto the Redpanda stream (`verdict.stream`). The scorer
(`verdict.scoring`) consumes in batches, reads the champion through a pointer that doubles as
the rollback flag (`verdict.models`), lets the challenger score the same features in shadow,
and stages every decision with the features it was served (`verdict.history`). From history
the review queue is ranked by expected loss (`verdict.review_queue`) and each finished day is
judged by the drift monitors and the models job (`verdict.drift`, `verdict.live`), which open
retraining and promotion pull requests and never merge them. The scorer's per-hop metrics go
to Prometheus and the public Grafana dashboard (`verdict.observe`), which is the first link on
the static page at fraud.peterparker.ca (`verdict.publish`, `site/`).

## One transaction through the scorer

![A transaction is consumed in a batch, deduplicated, served its features before it is observed, scored by the champion with the challenger in shadow, decided, persisted and checkpointed only after its decision is on the stream; the p99 per hop is from the published load test on the live instance.](architecture/scoring-path.svg)

The decision path runs top to bottom: a batch is consumed from the stream, a ledger stops a
transaction that has already been seen, the feature engine serves the event before it observes
it, the ONNX champion scores it while the challenger scores in shadow, the rules turn the score
into an action that names the model that made it, and the decision is persisted, produced to
the stream and only then checkpointed, so a stop never splits a batch. The figures under each
hop are the 99th percentile from the published load test on the live instance at a thousand
transactions a second, which comes to 8.4 ms end to end against the 50 ms budget. Beside the
path: a record the scorer cannot decide goes to a dead-letter topic with its bytes untouched;
the feature state is saved to the data volume a slice at a time between batches so a
replacement instance restores rather than rebuilds (`verdict.scoring.recovery`); the champion
pointer is read on every event, which is what makes a rollback take effect on the next one
(`verdict.scoring.flags`); and timings per hop and per batch are exported as Prometheus metrics
(`verdict.observe.metrics`).

## How the published numbers are produced

![The real-data track replays the competition data offline for the leakage test, the training sets, the promotion gate and the queue evaluation; the synthetic track plays a sealed regime schedule at a fixed rate on the live instance for latency, throughput and drift; every table in the README is written from a committed report.](architecture/measurement.svg)

Every number in the README comes from one of two tracks, and each table says which. The
real-data track (left) is offline: the competition rows become events with nothing invented to
fill the schema, the leakage test (`verdict.store.leakage`) recomputes every feature from the
raw log and fails on any value an event could not have known, training sets are cut at a date
and hold only the labels that had arrived by it (`verdict.models.dataset`), the promotion gate
(`verdict.models.promote`) compares champion and challenger by a paired bootstrap on the
interval bound and writes its verdict into a pull request, and the queue evaluation
(`verdict.review_queue.evaluate`) pays expected-loss ranking against score ranking a day at a
time. The synthetic track (right) is live and under load: the generator's regimes are sealed
by committed hashes with the secret revealed after the window, the load test
(`verdict.scoring.loadtest`) sends at fixed rates and times every hop over five runs, the
scorer runs on the live instance with the champion, the shadow and history on, the parity
checks (`verdict.stream.parity`) hold the online store to the offline store and each stream to
a run with no stream, and drift is judged daily against the window's own first days
(`verdict.drift`). Both tracks end in committed JSON reports with 95% intervals across runs,
and `verdict observe report` writes the README tables from them in one command.

## The live window

![Terraform from the laptop stands up an auto-scaling group of one in AWS ca-central-1 that boots the pushed image, keeps history and saved feature state on an EBS volume, serves the dashboard through a Cloudflare tunnel, emails alerts through SNS and opens its retraining and promotion verdicts as pull requests a person merges.](architecture/live.svg)

The live stack is one region and one instance with no inbound port. From the laptop,
`deploy/push-image.sh` pushes a single image to ECR and `deploy/terraform` with
`deploy/go-live.sh` applies the auto-scaling group of one, whose boot script reads the sealed
schedule secret and the GitHub token from SSM and pulls the image. The instance runs the whole
platform under Docker Compose (`deploy/live/compose.yml`): Redpanda, the scorer, the two feeds,
the labels job, the compactor, the models job and the alerts job. History and the scorer's saved
feature state live on an EBS volume that survives a replacement. Prometheus and an anonymous
read-only Grafana serve the public dashboard through a Cloudflare tunnel, the only way out.
Alerts such as a day that did not finalise or a history that was not sealed go to an SNS topic
that emails Peter; the models job opens retraining and promotion-gate pull requests on the
GitHub repository, and a person merges or closes them. `deploy/down.sh` tears the stack down and
asks the cloud, not Terraform's state, what is left.
