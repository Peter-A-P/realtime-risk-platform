# The live stack: one region, one availability zone, one spot instance in an
# auto-scaling group of one, and a data volume that outlives the instance.
# ADR 14 records the shape and why; ADR 3 why the stream is Redpanda on the
# instance rather than Kinesis.
#
# Every resource is tagged project=verdict. The account is shared with
# project 04, so the budget (verdict-monthly, made outside Terraform so it
# exists before and after the stack) and the teardown check both count only
# what carries that tag.

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    project      = "verdict"
    "managed-by" = "terraform"
  }

  # Pinned, with checksums where the boot script downloads a binary. A stack
  # that upgrades itself under a latency measurement is not a measurement.
  compose_version = "v5.5.1"
  compose_sha256  = "db1889184726840f75c4f9c001048430d4f25b3be3cb084d3ddd762bc0aed576"
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}
