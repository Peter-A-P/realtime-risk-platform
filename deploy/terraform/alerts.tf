# Alerts reach Peter by email (ADR 26). Prometheus on the instance evaluates
# the rules; the alerts container writes what fires to an outbox; the host
# publishes it here as the instance's role. SNS email is free to the first
# thousand a month, and the topic is tagged, so down.sh removes and checks it
# like everything else.

resource "aws_sns_topic" "alerts" {
  name = "verdict-alerts"
}

# The address is not committed: the repository is public. Pass it with
# TF_VAR_alert_email, and confirm the subscription from the email AWS sends.
# Without it the topic still exists and alerts are published to nobody.
resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
