# One spot instance, kept at one by an auto-scaling group. A spot
# interruption terminates the instance; the group launches a replacement in
# the same zone, and its boot script reattaches the data volume and starts
# the stack. ADR 15 records how the feeds recover, and what is open.

# Amazon Linux 2023, x86, the current image when Terraform runs. The build is
# tested on x86 (PLAN.md section 6), so the live stack is too.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_launch_template" "instance" {
  name                   = "verdict"
  image_id               = data.aws_ssm_parameter.al2023.value
  update_default_version = true

  iam_instance_profile {
    arn = aws_iam_instance_profile.instance.arn
  }

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [aws_security_group.instance.id]
    delete_on_termination       = true
  }

  # The root volume is disposable: everything that must survive is on the
  # data volume.
  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size           = 16
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  # IMDSv2 only, and a hop limit of 1 so a container on the bridge network
  # cannot reach the instance's credentials. Only the boot script, on the
  # host, needs them.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  # Gzipped whole, which cloud-init recognises by its magic bytes and
  # unpacks: EC2 limits user data to 16 KB, and the boot script with the
  # compose file inside it outgrew that on 2026-09-27 (ADR 26's note).
  user_data = base64gzip(templatefile("${path.module}/boot.sh.tftpl", {
    region          = var.region
    volume_id       = aws_ebs_volume.data.id
    compose_version = local.compose_version
    compose_sha256  = local.compose_sha256
    # Gzipped: user data is limited to 16 KB, and a test keeps it inside.
    compose_b64gz   = base64gzip(file("${path.module}/../live/compose.yml"))
    window_start    = var.window_start
    schedule        = var.schedule
    commitment_b64  = var.schedule == "sealed" ? filebase64(local.commitment) : ""
    image           = "${aws_ecr_repository.verdict.repository_url}:${var.image_tag}"
    registry        = split("/", aws_ecr_repository.verdict.repository_url)[0]
    alert_topic_arn = aws_sns_topic.alerts.arn
  }))

  lifecycle {
    precondition {
      condition     = var.schedule != "sealed" || fileexists(local.commitment)
      error_message = "schedule = sealed needs docs/sealed-schedule.json: run verdict schedule seal first."
    }
  }

  # default_tags does not reach what an auto-scaling group launches, so the
  # instance, its root volume and its network interface are tagged here. The
  # budget and the teardown check both depend on it.
  dynamic "tag_specifications" {
    for_each = ["instance", "volume", "network-interface"]

    content {
      resource_type = tag_specifications.value
      tags          = merge(local.tags, { Name = "verdict" })
    }
  }
}

resource "aws_autoscaling_group" "instance" {
  name                      = "verdict"
  min_size                  = 0
  max_size                  = 1
  desired_capacity          = var.running ? 1 : 0
  vpc_zone_identifier       = [aws_subnet.public.id]
  health_check_type         = "EC2"
  health_check_grace_period = 300

  # Off on purpose. Rebalancing launches a replacement before the old
  # instance goes, and the old one still holds the data volume, so the new
  # one could not attach it. One instance at a time.
  capacity_rebalance = false

  mixed_instances_policy {
    instances_distribution {
      on_demand_base_capacity                  = 0
      on_demand_percentage_above_base_capacity = 0
      # Capacity first, then the list's order, and price not at all (ADR 20,
      # third amendment): the pools least likely to be interrupted, where
      # price-capacity-optimized kept choosing ones reclaimed several times
      # a day.
      spot_allocation_strategy = "capacity-optimized-prioritized"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.instance.id
        version            = "$Latest"
      }

      dynamic "override" {
        for_each = var.instance_types

        content {
          instance_type = override.value
        }
      }
    }
  }

  dynamic "tag" {
    for_each = merge(local.tags, { Name = "verdict" })

    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
}
