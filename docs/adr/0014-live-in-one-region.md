# 14. The live stack: one region, one zone, one spot instance, nothing inbound

- Status: accepted, 2026-09-18. The open question at the end (storage) was
  **closed on 2026-09-19 by ADR 18**: topics keep a day, and history is a
  labelled, weighted sample. The data volume moved from 100 to 150 GB
- Date: 2026-09-18
- Deciders: Peter Parker (the account, the window, the instance size, the
  dashboard's host); the build session (the rest, below)

## Context

`PLAN.md` section 2.8 put the whole live stack in AWS `ca-central-1` on one
spot instance behind an auto-scaling group of one, with state on a separate
volume. Three things moved on 2026-09-18, all recorded in `PLAN.md`'s header:

- the stream on the instance is Redpanda, not Kinesis (ADR 3, amended);
- the instance is a c6a.large, 2 vCPU and 4 GB, for a sixty-day window;
- the AWS account is the one project 04 uses. 09 shares the account and
  nothing else, so everything 09 owns has to be findable, countable and
  removable by its tag.

And one fact was new: the domain `peterparker.ca` is on Cloudflare, where
`coach.peterparker.ca` is already served through a Cloudflare Tunnel.

This record is the shape of `deploy/terraform/`, what each part is for, and
what it deliberately leaves out.

## Options considered

Where the dashboard is served from:

1. **An Azure Static Web App**, as 01, 02 and 08 do. Those are static result
   pages; this dashboard shows telemetry that exists only on the instance. A
   static page would still need a public API on the instance, CORS, and a
   second cloud to deploy and tear down. Rejected.
2. **An open port on the instance**, with an Elastic IP and a DNS record.
   Every spot replacement changes nothing only if the Elastic IP moves with
   it, the port is open to the internet, and TLS is this project's to run.
   Rejected.
3. **A Cloudflare Tunnel from the instance.** The instance dials out; nothing
   dials in. TLS is Cloudflare's, the DNS record never changes, and a
   replacement instance reconnects with the same token. Chosen.

How a person reaches the instance: SSH with a key pair and port 22, or **SSM
Session Manager**, which the instance dials out to. Session Manager, so the
security group has no ingress rule at all.

How the code reaches the instance: build on the instance from a clone (the
repository is private until go-live, so the instance would need a GitHub
credential), or **an image in ECR in the same account**, pulled by the
instance's own role. ECR.

## Decision

`deploy/terraform/`, fifteen resources, every one tagged `project=verdict`
and `managed-by=terraform`:

| Part | What | Why |
|---|---|---|
| Network | Its own VPC (`10.90.0.0/24`), one public subnet in `ca-central-1d`, an internet gateway | Nothing of 04's shares a boundary with it, and everything is destroyed with it. Public so the instance reaches ECR, SSM and Cloudflare without a NAT gateway, which would cost more than the instance |
| Security group | Egress only. **No ingress rule**; a test asserts there is never one | The dashboard leaves by the tunnel, a person arrives by Session Manager |
| Data volume | gp3, encrypted, 100 GB provisional (see the open question), in the same zone | Outlives every instance. The boot script attaches it, makes a filesystem only if there is none, and mounts it at `/data` |
| Image | ECR repository `verdict`, immutable tags, scan on push, the last ten kept | An image a measurement ran on cannot be replaced under its name. `force_delete` so teardown can remove it |
| Instance role | Read `/verdict/*` parameters, attach this volume to a tagged instance, pull this image, Session Manager | Nothing else. It cannot create resources or read another project's parameters |
| Launch template | Amazon Linux 2023 x86, IMDSv2 only with a hop limit of 1, a disposable 16 GB root, the boot script as user data, tags on the instance, its volume and its interface | Containers cannot reach the instance's credentials. `default_tags` does not reach what a group launches, so the template tags it |
| Group of one | Spot only, price-capacity-optimized over eight 4 vCPU, 16 GB x86 types (m7i, m6i, m5, m6a, m5a, m6in, m6id and m5d, all xlarge, since ADR 20's second amendment of 2026-09-22; m7i.xlarge, m6i.xlarge and m5.xlarge from 2026-09-21; r7i.large, r6i.large and r5.large before that, and c6a.large, c5a.large and c7i.large first); capacity rebalancing **off** | Three types so a shortage of one is not an outage. Rebalancing would start a replacement while the old instance still holds the volume |
| Switch | `running` (default false) sets the group's desired capacity | Everything can exist with the instance off, costing only the volume |

Around it, outside Terraform on purpose:

- **The budget** `verdict-monthly` (US$60, filtered to the tag, alerts at 50,
  80, 100 percent actual and 100 forecast) was made from the CLI so it exists
  before the first resource and after the last.
- **The tunnel token** is a `SecureString` in SSM Parameter Store at
  `/verdict/cloudflare-tunnel-token`, put there by Peter. The boot script
  reads it into a root-only file on the disposable root volume. It never
  passes through Terraform, so the state holds no secret.
- **Two identities' worth of permissions**, one user:
  `deploy/aws/iam/bootstrap-policy.json` (read the account, make the budget)
  and `deploy/aws/iam/deploy-policy.json` (what `terraform apply` needs). The
  deploy policy creates EC2 and group resources only with the project tag in
  the request, changes only resources that carry it, passes only roles under
  `/verdict/`, and attaches only the Session Manager managed policy. Tests
  assert the first and third of those.
- **Terraform state** is local and ignored by git. Losing it would orphan
  resources, which is why `deploy/down.sh` asks the AWS tagging API, not the
  state, what is left after a teardown, and fails if anything tagged remains
  but the tunnel token.

`deploy/live/compose.yml` is the stack on the instance: the broker, the
topics job and the tunnel, and since 2026-09-19 the scorer, the label
collector, the hourly compactor (ADR 18) and Prometheus. The live generator
and its label feed, and Grafana, are still to come. Its topics must match the
local stack's names and partition counts exactly, and a test holds them
together.

**The image (added 2026-09-19).** `deploy/image/Dockerfile` builds one image
that every platform service runs with a different command, as an
unprivileged user (uid 10001). `.dockerignore` admits only `pyproject.toml`,
`README.md`, `LICENSE` and `verdict/`, so `data/` cannot reach a registry.
`deploy/push-image.sh` refuses a dirty tree, tags the image with the commit
and pushes it to the immutable repository; Terraform's `image_tag` variable
names it, and `running = true` without one is refused at plan time. The boot
script logs the instance in to ECR with its own role and gives each service
its directory on the data volume by uid. CI builds the image and checks it
runs as that user; nothing pushes from CI.

## Open question: at 1,000 events a second, history does not fit on a disk

**Closed 2026-09-19 by ADR 18, option 1 below.** Kept as it was asked.

Measured on 20,000 generated events on 2026-09-18, as JSON on the wire:

| Stream | Bytes per record | Per day at 1,000 a second |
|---|---:|---:|
| Transactions | 367 | 31.7 GB |
| Labels (one per transaction) | 142 | 12.3 GB |
| Decisions | about 250, estimated from its fields, not measured | about 21.6 GB |
| Shadow decisions | the same | about 21.6 GB |

At the provisional retentions in `deploy/live/compose.yml` (two days of
transactions, nine of labels, two of each kind of decision) that is about
260 GB before compression. Over the sixty-day window the platform sees about
5.2 billion transactions: about 1.9 TB of them alone. `PLAN.md` section 6
priced a 50 GB volume.

Compression helps less than the file-level ratio suggests. gzip at level 1
takes the transaction file to a tenth of its size and the label file to a
sixteenth, but the producer on the latency path lingers 2 ms, so at 1,000
events a second a batch holds about two records and compresses little.
Topics off the latency path (labels, decisions, shadow) can batch longer and
compress well.

What does not change: the transactions topic must hold at least the longest
feature window (24 hours, `verdict/store/features.py`) plus the time a
replacement instance needs to replay it, because the online state is rebuilt
by replay (ADR 4). That is about 32 GB whatever else is decided.

The options, to be decided before go-live:

1. **Topics as buffers, a sink for history, and a published sample.** Each
   topic keeps a day or two. A sink writes what must be kept to Parquet with
   zstd: every fraud, every item the review queue sees, and a hash sample of
   legitimate traffic, with its sampling weight stored beside it. The
   promotion gate (ADR 11) and the queue evaluation (ADR 13) then read
   weighted rows, and both have to be shown to give the same answer on a
   replay with and without sampling. The recommendation.
2. **A disk big enough for everything.** Two terabytes of gp3 is about
   US$176 a month: more than the rest of the live stack together, for the
   same numbers. Rejected on cost.
3. **A lower live rate.** The one-line claim is thousands a second. Rejected
   unless the load test forces it.

Until it is decided the volume is 100 GB, which holds option 1 comfortably,
and gp3 volumes grow in place without detaching, so the provisional size
costs nothing to change.

**The public dashboard (added 2026-09-19).** Grafana on the instance,
reached only through the tunnel, which must route `risk.peterparker.ca` to
`http://grafana:3000` (the tunnel runs in the stack's network, so
`localhost` would be the tunnel's own container). Anonymous viewers get the
one provisioned dashboard, read-only; the login form, basic auth, sign-up,
Explore, snapshots and public-dashboard sharing are all off, and the admin
password is random per boot and written nowhere else. The dashboard is code
(`verdict/observe/dashboard.py`), written into Grafana's provisioning by a
one-shot job from the platform image, and `tests/test_dashboard.py` checks
every query's metric against what the scorer and the feeds register, shown
failing on a renamed metric. Because a viewer's panel is a Prometheus query,
Prometheus bounds every query (10 seconds, five million samples, four at a
time), so a public page cannot spend the instance's CPU. The scorer gains
one metric for it, event time to decision, which on the live stack is ingest
to decision because the feed sends each transaction at its event time.

## Consequences

- The stack costs the volume (about US$9 a month at 100 GB) while the
  instance is off, and about US$45 a month with it on (the instance, both
  volumes, its public IPv4 address and a little egress). `PLAN.md` section 6
  carries the figures.
- Spot recovery has a shape but not yet evidence: the replacement waits for
  the volume, attaches it, and restarts the stack. How the platform rebuilds
  its online state, and how long that takes, is ADR 15, with a timed
  interruption drill.
- Teardown has a check but not yet a test in CI: `deploy/down.sh --check`
  needs the deploy identity, and CI does not hold one. ADR 16 decides whether
  it should.
- The boot script installs Docker from the distribution and Compose as a
  pinned, checksummed binary, on every replacement. A few minutes of every
  recovery go to that. Baking an image would save them and add a build step
  to keep current; not yet.
- `.gitattributes` keeps the boot script and the live compose file LF on a
  Windows checkout. A carriage return there is a boot that fails on AWS, and
  a test checks for one.

## Sources

- Amazon EC2 Auto Scaling, groups with multiple instance types and purchase
  options, and the price-capacity-optimized strategy.
  https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-mixed-instances-groups.html
- Capacity rebalancing, and why it launches before it terminates.
  https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-capacity-rebalancing.html
- Instance metadata service version 2 and the response hop limit.
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-IMDS-existing-instances.html
- Amazon EBS and NVMe: device names and the volume id in the serial.
  https://docs.aws.amazon.com/ebs/latest/userguide/nvme-ebs-volumes.html
- AWS Systems Manager Session Manager.
  https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html
- Amazon EBS pricing (gp3) and the charge for public IPv4 addresses.
  https://aws.amazon.com/ebs/pricing/ and https://aws.amazon.com/vpc/pricing/
- Cloudflare Tunnel, remotely managed tunnels and their token.
  https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/
- Terraform AWS provider, `default_tags` and resources created indirectly.
  https://registry.terraform.io/providers/hashicorp/aws/latest/docs#default_tags-configuration-block
