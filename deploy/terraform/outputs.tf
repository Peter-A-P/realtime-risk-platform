output "autoscaling_group" {
  description = "The group of one. Its desired capacity is the stack's on switch."
  value       = aws_autoscaling_group.instance.name
}

output "data_volume_id" {
  description = "The volume that outlives every instance."
  value       = aws_ebs_volume.data.id
}

output "image_repository" {
  description = "Where the platform's image is pushed."
  value       = aws_ecr_repository.verdict.repository_url
}

output "alerts_topic" {
  description = "Where alerts are published (ADR 26)."
  value       = aws_sns_topic.alerts.arn
}

output "archive_bucket" {
  description = "Where deploy/archive/fetch.sh moves the window's record through."
  value       = aws_s3_bucket.archive.bucket
}
