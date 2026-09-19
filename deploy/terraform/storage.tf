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
