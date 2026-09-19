# The instance's own identity. It can read its parameters, attach its one data
# volume, pull its one image, and be reached through Session Manager. Nothing
# else: it cannot create resources, read other parameters, or touch project
# 04's.

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "verdict-instance"
  path               = "/verdict/"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

# Session Manager, instead of SSH and an open port.
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "instance" {
  statement {
    sid       = "ReadOwnParameters"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/verdict/*"]
  }

  statement {
    sid     = "AttachTheDataVolume"
    actions = ["ec2:AttachVolume"]
    resources = [
      aws_ebs_volume.data.arn,
      "arn:${data.aws_partition.current.partition}:ec2:${var.region}:${data.aws_caller_identity.current.account_id}:instance/*",
    ]

    # The instance half of the pair: only this project's instances.
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/project"
      values   = ["verdict"]
    }
  }

  statement {
    sid       = "SeeVolumeState"
    actions   = ["ec2:DescribeVolumes"]
    resources = ["*"]
  }

  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid = "PullOwnImage"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [aws_ecr_repository.verdict.arn]
  }
}

resource "aws_iam_role_policy" "instance" {
  name   = "verdict-instance"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.instance.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "verdict-instance"
  path = "/verdict/"
  role = aws_iam_role.instance.name
}
