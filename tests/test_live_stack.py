"""The live stack's promises, checked from its files before it ever runs.

Everything here is a property that would otherwise be found on AWS, at the
price of an instance hour or a public mistake:

- the live topics are the local topics, so the code tested here is the code
  that runs there;
- nothing is reachable from outside: no ingress rule, no published port;
- the instance's credentials are out of reach of its containers (IMDSv2,
  hop limit 1);
- the files that run on Linux carry no carriage return, which on Windows
  checkouts is one autocrlf away from a boot script that fails on its first
  line;
- the deploy identity cannot create an untagged resource, because the budget
  and the teardown check both see only what is tagged.

`deploy/down.sh --check` is the other half: it asks the AWS API, not these
files, what is still tagged after a teardown.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
LOCAL_COMPOSE = ROOT / "deploy" / "compose" / "docker-compose.yml"
LIVE_COMPOSE = ROOT / "deploy" / "live" / "compose.yml"
TERRAFORM = ROOT / "deploy" / "terraform"
DEPLOY_POLICY = ROOT / "deploy" / "aws" / "iam" / "deploy-policy.json"
BOOTSTRAP_POLICY = ROOT / "deploy" / "aws" / "iam" / "bootstrap-policy.json"

_ENSURE = re.compile(r"^\s*ensure ([a-z-]+) (\d+) (\d+)\s*$", re.MULTILINE)


def _compose(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _topics(path: Path) -> dict[str, int]:
    """Topic name to partition count, from the compose file's topics job."""
    script = "\n".join(_compose(path)["services"]["topics"]["command"])
    found = {name: int(partitions) for name, partitions, _ in _ENSURE.findall(script)}
    assert found, f"no topics found in {path}"
    return found


def _terraform_code() -> str:
    """Every .tf file, with comments removed so a comment cannot trip a check."""
    text = "\n".join(p.read_text(encoding="utf-8") for p in sorted(TERRAFORM.glob("*.tf")))
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def test_the_live_topics_are_the_local_topics() -> None:
    """Same names, same partition counts. Retentions differ on purpose."""
    assert _topics(LIVE_COMPOSE) == _topics(LOCAL_COMPOSE)


def test_the_transactions_topic_is_one_partition_live() -> None:
    """The feature engine needs time order, and one partition is how (ADR 8)."""
    assert _topics(LIVE_COMPOSE)["transactions"] == 1


def test_no_live_service_publishes_a_port() -> None:
    services = _compose(LIVE_COMPOSE)["services"]
    published = {name: s["ports"] for name, s in services.items() if "ports" in s}
    assert published == {}


def test_no_security_group_accepts_inbound_traffic() -> None:
    code = _terraform_code()
    assert "ingress" not in code
    assert "aws_vpc_security_group_ingress_rule" not in code


def test_containers_cannot_reach_the_instance_credentials() -> None:
    code = _terraform_code()
    assert re.search(r'http_tokens\s*=\s*"required"', code)
    assert re.search(r"http_put_response_hop_limit\s*=\s*1\b", code)


def test_what_an_autoscaling_group_launches_is_tagged() -> None:
    """default_tags stops at the group; the launch template carries the rest."""
    code = _terraform_code()
    for resource_type in ("instance", "volume", "network-interface"):
        assert f'"{resource_type}"' in code


@pytest.mark.parametrize(
    "path",
    [
        TERRAFORM / "boot.sh.tftpl",
        LIVE_COMPOSE,
        ROOT / "deploy" / "up.sh",
        ROOT / "deploy" / "down.sh",
        ROOT / "deploy" / "push-image.sh",
    ],
)
def test_files_that_run_on_linux_have_no_carriage_returns(path: Path) -> None:
    assert b"\r" not in path.read_bytes()


def _statements(path: Path) -> list[dict[str, Any]]:
    policy = json.loads(path.read_text(encoding="utf-8"))
    statements = policy["Statement"]
    assert isinstance(statements, list)
    return statements


@pytest.mark.parametrize("path", [DEPLOY_POLICY, BOOTSTRAP_POLICY])
def test_no_policy_grants_a_whole_service(path: Path) -> None:
    for statement in _statements(path):
        actions = statement["Action"]
        for action in [actions] if isinstance(actions, str) else actions:
            assert action != "*"
            service, _, verb = action.partition(":")
            assert verb != "*", f"{statement['Sid']} grants all of {service}"


def test_the_deploy_identity_creates_only_tagged_resources() -> None:
    """Every EC2 or group create is conditioned on the project tag."""
    for statement in _statements(DEPLOY_POLICY):
        creates = [
            a
            for a in statement["Action"]
            if a.startswith(("ec2:Create", "autoscaling:Create"))
            and a not in {"ec2:CreateTags", "autoscaling:CreateOrUpdateTags", "ec2:CreateRoute"}
            and a != "ec2:CreateLaunchTemplateVersion"
        ]
        if not creates:
            continue
        condition = statement.get("Condition", {}).get("StringEquals", {})
        assert condition.get("aws:RequestTag/project") == "verdict", statement["Sid"]


def test_the_deploy_identity_passes_only_its_own_role() -> None:
    passing = [s for s in _statements(DEPLOY_POLICY) if "iam:PassRole" in s["Action"]]
    assert len(passing) == 1
    assert passing[0]["Resource"] == "arn:aws:iam::*:role/verdict/*"
    assert passing[0]["Condition"]["StringEquals"]["iam:PassedToService"] == "ec2.amazonaws.com"


def test_tag_on_create_names_actions_without_a_service_prefix() -> None:
    """`ec2:CreateAction` holds `CreateVpc`, not `ec2:CreateVpc`.

    With the prefix the condition never matches, every tagged create is
    refused, and the deploy fails on its first resource. It did, on
    2026-09-19, in a dry run before anything was applied.
    """
    for statement in _statements(DEPLOY_POLICY):
        values = statement.get("Condition", {}).get("StringEquals", {}).get("ec2:CreateAction", [])
        for value in values:
            assert ":" not in value, f"{statement['Sid']}: {value}"


def test_every_platform_service_runs_the_one_pushed_image() -> None:
    """One image, named by the commit it was built from (deploy/push-image.sh)."""
    services = _compose(LIVE_COMPOSE)["services"]
    ours = {name: s for name, s in services.items() if "VERDICT_IMAGE" in str(s.get("image"))}
    assert set(ours) == {
        "scorer",
        "labels",
        "compactor",
        "feed-transactions",
        "feed-labels",
        "grafana-files",
    }
    assert len({s["image"] for s in ours.values()}) == 1


def test_the_scorer_stages_its_decisions_on_the_data_volume() -> None:
    """ADR 18: history lives on the volume that outlives the instance."""
    scorer = _compose(LIVE_COMPOSE)["services"]["scorer"]
    assert "--history=/data/history" in scorer["command"]
    assert "/data/history:/data/history" in scorer["volumes"]


def test_the_image_leaves_the_data_directory_out() -> None:
    """An image pushed to a registry is a redistribution (docs/data.md)."""
    ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert ignore[ignore.index("*")] == "*"
    assert not any(line.startswith("!data") for line in ignore)


def test_the_public_dashboard_is_anonymous_read_only_and_closed_to_login() -> None:
    grafana = _compose(LIVE_COMPOSE)["services"]["grafana"]
    env = grafana["environment"]
    assert env["GF_AUTH_ANONYMOUS_ENABLED"] == "true"
    assert env["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert env["GF_AUTH_DISABLE_LOGIN_FORM"] == "true"
    assert env["GF_USERS_ALLOW_SIGN_UP"] == "false"
    assert env["GF_EXPLORE_ENABLED"] == "false"
    assert "ports" not in grafana
    assert all(volume.endswith(":ro") for volume in grafana["volumes"])


def test_a_public_query_cannot_hold_the_instance() -> None:
    command = _compose(LIVE_COMPOSE)["services"]["prometheus"]["command"]
    assert any(arg.startswith("--query.timeout=") for arg in command)
    assert any(arg.startswith("--query.max-samples=") for arg in command)


def test_both_feeds_play_the_same_stream_and_keep_their_place_on_the_volume() -> None:
    services = _compose(LIVE_COMPOSE)["services"]
    feeds = [services["feed-transactions"], services["feed-labels"]]

    def shared(command: list[str]) -> list[str]:
        return [a for a in command if a.startswith(("--start=", "--schedule=", "--state="))]

    assert shared(feeds[0]["command"]) == shared(feeds[1]["command"])
    for feed in feeds:
        assert "--state=/data/feeds" in feed["command"]
        assert "/data/feeds:/data/feeds" in feed["volumes"]


def test_the_user_data_fits_the_sixteen_kilobyte_limit() -> None:
    """The boot script carries the compose file gzipped; both must fit."""
    import base64
    import gzip

    boot = (TERRAFORM / "boot.sh.tftpl").read_bytes()
    compose = base64.b64encode(gzip.compress(LIVE_COMPOSE.read_bytes()))
    assert len(boot) + len(compose) + 2_048 < 16_384


def test_the_build_identity_can_never_read_or_replace_the_schedule_secret() -> None:
    """The sealed schedule is only worth something if the builder cannot learn it.

    The commitment proves the schedule was not changed after sealing. It
    does not stop someone who can read the secret from knowing the regime
    days in advance, and the identity the build runs as may otherwise read
    and write every parameter under /verdict/. An explicit deny wins over
    that allow, so only the instance's own role reads the secret, and only
    Peter, from outside this project's identities, writes it.
    """
    denied = [
        statement
        for statement in _statements(BOOTSTRAP_POLICY)
        if statement["Effect"] == "Deny"
        and statement["Resource"].endswith(":parameter/verdict/schedule-secret")
    ]
    assert len(denied) == 1
    actions = set(denied[0]["Action"])
    assert {"ssm:GetParameter", "ssm:GetParameters", "ssm:PutParameter"} <= actions
    assert "ssm:GetParameterHistory" in actions
