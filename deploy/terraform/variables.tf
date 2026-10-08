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

variable "on_demand" {
  description = "Run the one instance on demand, which AWS does not reclaim, rather than on spot (ADR 31). True from 2026-09-29."
  type        = bool
  default     = true
}

variable "instance_types" {
  description = "Candidates in order of preference, all x86 with 4 vCPU and 32 GB (ADR 31: the scorer outgrew 16 GB once a day of features had built up; ADR 20's 2026-09-21 amendment: two vCPUs are too few for scorer, broker and feed). On demand the group takes the first it can launch."
  type        = list(string)
  # 32 GB, not 16 (ADR 31): on the live window's second day the scorer held
  # 13 GB or more before a restore and about 15 GB after, with 2 GB more for
  # the broker, feeds and dashboard, on a 16 GB machine. The spot lists of
  # ADR 20's second and third amendments are in the git history.
  default = ["r6i.xlarge", "r7i.xlarge", "r5.xlarge"]
}

variable "data_volume_gb" {
  description = "The data volume. Sized in ADR 18 from measured bytes per row: a day of each topic, eight days of staged decisions and labels, and the window's kept sample. 150 until 2026-10-08, when the kept sample measured 1.33 GB a live day and the volume was grown online to 200 (ADR 18, third addendum)."
  type        = number
  default     = 200
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
