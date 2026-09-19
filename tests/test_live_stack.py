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
