# The data volume holds the broker's log and everything the platform keeps
# between decisions. It is a separate volume, not the instance's root, so a
# spot interruption loses the instance and keeps the data: the replacement
# attaches it at boot (boot.sh.tftpl). ADR 14 sizes it.

resource "aws_ebs_volume" "data" {
  availability_zone = var.availability_zone
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true

  tags = { Name = "verdict-data" }
}

# The platform's image. Built and pushed from the build machine; the instance
# pulls it with its own role. Tags are immutable, so an image a measurement
# ran on cannot be replaced under its name. force_delete lets down.sh remove
# the repository with images still in it: teardown is part of done.
resource "aws_ecr_repository" "verdict" {
  name                 = "verdict"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "verdict" {
  repository = aws_ecr_repository.verdict.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last ten images; storage is billed per GB-month"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# Where the window's record leaves the instance for the build machine before
# teardown (deploy/archive): the metrics behind the dashboard and the kept
# history. Private, and only the instance writes to it. Objects expire after
# thirty days, and force_destroy lets down.sh remove it with objects still in
# it, so an archive nobody fetched is not billed for long.
resource "aws_s3_bucket" "archive" {
  bucket        = "verdict-archive-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "expire"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
