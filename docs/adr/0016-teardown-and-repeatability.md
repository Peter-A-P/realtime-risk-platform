# 16. Teardown and repeatability: one command each way, and the cloud is asked what is left

- Status: accepted, 2026-09-27
- Date: 2026-09-27
- Deciders: the build session, within `PLAN.md` section 10 ("stack up and
  down on one command; nothing tagged `project=verdict` billable the day
  after it ends") and `CLAUDE.md` ("Teardown is part of done")

## Context

The live stack is a VPC, one spot instance in a group of one, a 150 GB data
volume, an ECR repository, an SNS topic, an instance role and a handful of
parameters in a shared AWS account that also holds project 04 (ADR 14). The
plan requires that one command brings it up, one takes it down, and that
the day after the live window nothing of this project's is billable. ADR 14
left open whether the teardown check should run in CI.

Two things make teardown easy to get wrong here. Terraform's state is local
and not committed, so a lost or stale state file makes `terraform destroy`
report success while resources remain. And some things are meant to outlive
the stack: parameters put in SSM by hand, one of which the reveal needs.

## Decision

**`deploy/up.sh` and `deploy/down.sh` are the two commands.** `up.sh` applies
Terraform with the image, the window's start and the schedule; Terraform
shows the plan and asks. `down.sh` destroys, then **asks AWS, not the state**:
it lists every resource tagged `project=verdict` through the Resource Groups
Tagging API and fails if anything remains beyond what is meant to. Every
resource the deploy identity can create must carry the tag, which the IAM
policy enforces on create (`tests/test_live_stack.py` holds the policy to
it), so the tag is a complete inventory.

**What is meant to outlive the stack, and why:**

| What | Why it stays | Cost |
|---|---|---|
| `/verdict/cloudflare-tunnel-token` | A dashboard for a later re-run uses the same tunnel | Free (standard parameter) |
| `/verdict/schedule-secret` | The reveal the day after the window reads it; a stranger re-deriving the schedule needs the secret, which Peter also keeps | Free (standard parameter) |
| `verdict-monthly` budget | Watches the tag at no charge; it is how "nothing billable" is seen | Free (first two budgets) |

**What is not, and is reported if left:** everything else, and in particular
`/verdict/github-token` (ADR 28), which is a credential: delete it and revoke
it on GitHub. `tests/test_live_stack.py` holds `down.sh`'s allowance to
exactly the two parameters above.

**The data volume is destroyed with the stack.** What the window proves is
committed before teardown: the live report (`verdict observe report`, ADR
25), the drift, retraining and gate pull requests (ADR 28), and the cost
from the bill. Raw history is synthetic and reproducible from the revealed
schedule; it is not kept.

**The check is not run in CI.** It needs an AWS identity, and CI holds none by
design: a leaked CI secret should never be one that can see the account. It
is run by hand at teardown, and its output is committed with the live
report. The day after, the budget's actual spend for the tag is the second
check.

**Repeatability** is by construction rather than by a second environment:
image tags are immutable and name the commit (`deploy/push-image.sh`), the
generator is deterministic from its seed and schedule, and the sealed
schedule is re-derivable from the revealed secret, so the live stream can
be regenerated exactly and any decision re-run on the image that made it.

## Evidence

- `deploy/down.sh`, and its filter checked on sample resource names: the two
  parameters are allowed, the GitHub token and a volume are reported.
- `tests/test_live_stack.py`: the deploy identity creates only tagged
  resources, and `down.sh` allows exactly the two parameters.
- The dry run brought the stack up with `up.sh` (2026-09-21) and has run it
  since; the teardown itself happens once, the day after the window, and its
  output is the evidence this ADR still waits for.

## Consequences

- A teardown that finds leftovers fails loudly, with their names, whatever
  Terraform's state says.
- The GitHub token has to be deleted and revoked by hand; `down.sh` names it
  until it is.
- Anything created outside Terraform and untagged (by hand in the console)
  is invisible to the check. The deploy identity cannot create untagged
  resources; a person with the account's own login can, and should not.

## Sources

- AWS Resource Groups Tagging API, `GetResources`:
  https://docs.aws.amazon.com/resourcegroupstagging/latest/APIReference/API_GetResources.html
- AWS Systems Manager Parameter Store pricing (standard parameters):
  https://aws.amazon.com/systems-manager/pricing/
- AWS Budgets pricing: https://aws.amazon.com/aws-cost-management/aws-budgets/pricing/
- ADR 14 (the live stack), ADR 25 (the live report), ADR 28 (the token).
