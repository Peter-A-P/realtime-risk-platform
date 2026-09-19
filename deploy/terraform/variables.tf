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
  description = "Spot candidates, all x86 with 2 vCPU and 4 GB, in order of preference. More than one so a shortage of one type is not an outage."
  type        = list(string)
  default     = ["c6a.large", "c5a.large", "c7i.large"]
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
