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
