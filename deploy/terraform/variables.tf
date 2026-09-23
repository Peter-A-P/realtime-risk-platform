variable "region" {
  description = "The one region the live stack runs in (PLAN.md 2.8)."
  type        = string
  default     = "ca-central-1"
}

variable "aws_profile" {
  description = "The named CLI profile. Its identity carries the bootstrap and deploy policies in deploy/aws/iam."
  type        = string
  default     = "verdict"
}

variable "availability_zone" {
  description = "The one zone. The data volume lives here and the instance must too, to reattach it. 1d had the lowest c6a.large spot price on 2026-09-18."
  type        = string
  default     = "ca-central-1d"
}

variable "instance_types" {
  description = "Spot candidates, all x86 with 4 vCPU and 16 GB (ADR 20: the engine holds about 6 GB at the live rate; its 2026-09-21 amendment: two vCPUs are one core, too few for scorer, broker and feed), in order of preference. More than one so a shortage of one type is not an outage."
  type        = list(string)
  # Eight pools, not three (ADR 20, second amendment): on 2026-09-22 the
  # group chose m6i.xlarge in ca-central-1d every time and it was reclaimed
  # five times in eleven hours. More pools give price-capacity-optimized
  # somewhere else to go. All x86; the d variants' local disks are unused.
  default = [
    "m7i.xlarge", "m6i.xlarge", "m5.xlarge", "m6a.xlarge",
    "m5a.xlarge", "m6in.xlarge", "m6id.xlarge", "m5d.xlarge",
  ]
}

variable "data_volume_gb" {
  description = "The data volume. Sized in ADR 18 from measured bytes per row: a day of each topic, eight days of staged decisions and labels, and the window's kept sample."
  type        = number
  default     = 150
}

variable "running" {
  description = "Whether the instance runs. False builds everything else and costs only the volume; true launches the one instance."
  type        = bool
  default     = false
}

variable "image_tag" {
  description = "The platform image to run, as deploy/push-image.sh printed it: a commit. Required when running."
  type        = string
  default     = ""

  validation {
    condition     = !var.running || var.image_tag != ""
    error_message = "running = true needs image_tag: push an image with deploy/push-image.sh first."
  }
}

variable "window_start" {
  description = "The live window's start, RFC 3339 with a zone. Fixed for the window: every feed restart continues from it. Required when running."
  type        = string
  default     = ""

  validation {
    condition     = !var.running || can(formatdate("YYYY", var.window_start))
    error_message = "running = true needs window_start as RFC 3339, for example 2026-10-01T00:00:00Z."
  }
}

variable "schedule" {
  description = "dev for a dry run on the public development schedule; sealed for the live window (the secret in SSM at /verdict/schedule-secret, the hashes in docs/sealed-schedule.json)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "sealed"], var.schedule)
    error_message = "schedule is dev or sealed."
  }
}

variable "alert_email" {
  description = "Where alerts are emailed (ADR 26). Never committed: set TF_VAR_alert_email. Empty publishes alerts to nobody."
  type        = string
  default     = ""
  sensitive   = true
}
