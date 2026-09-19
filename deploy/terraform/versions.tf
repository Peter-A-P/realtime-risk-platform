# Pinned majors, each with a reason, as pyproject.toml does for Python.
terraform {
  # 1.10 is the first release with ephemeral values and the S3 native lock this
  # configuration may move to; below 2 because a major can change the language.
  required_version = ">= 1.10, < 2.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # v6 is the current major. It moved region to a per-resource argument and
      # changed several defaults; the resources here are written against it.
      version = "~> 6.60"
    }
  }

  # State is local, in this directory, and ignored by git. It holds nothing
  # secret: the tunnel token is read by the instance at boot from SSM and never
  # passes through Terraform. Losing it orphans resources, which is why
  # down.sh checks the account by tag rather than trusting the state.
}
