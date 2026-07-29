# Deliberately small. Everything about pipeline health — freshness, quality
# verdicts, transform history — already lives in the stack's own ops.* schema
# and the Pipeline Health dashboard. What that cannot see is the host it runs
# on, so this covers exactly that: the instance is alive, the disk is not
# full, memory is not about to be reclaimed by the OOM killer.

locals {
  cw_agent_parameter_name = "/${var.project}/cloudwatch-agent-config"
}

resource "aws_sns_topic" "alarms" {
  name = "${var.project}-alarms"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Host failure recovers itself: EC2 stops and restarts the instance on new
# hardware, EBS volumes and the instance id survive.
resource "aws_cloudwatch_metric_alarm" "system_status" {
  alarm_name          = "${var.project}-system-status"
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"

  dimensions = { InstanceId = aws_instance.main.id }

  alarm_actions = [
    "arn:aws:automate:${var.region}:ec2:recover",
    aws_sns_topic.alarms.arn,
  ]
}

resource "aws_ssm_parameter" "cw_agent_config" {
  count = var.enable_cloudwatch_agent ? 1 : 0
  name  = local.cw_agent_parameter_name
  type  = "String"

  value = jsonencode({
    agent = {
      metrics_collection_interval = 300
    }
    metrics = {
      append_dimensions = { InstanceId = "$${aws:InstanceId}" }
      # The disk plugin tags each metric with path, device and fstype, and
      # CloudWatch matches dimension sets exactly — an alarm on
      # {InstanceId, path} binds to nothing unless that set is also published
      # here. Without this the /data alarm sits in INSUFFICIENT_DATA forever,
      # which looks identical to healthy.
      aggregation_dimensions = [["InstanceId"], ["InstanceId", "path"]]
      metrics_collected = {
        disk = {
          resources                   = ["/", "/data"]
          measurement                 = ["used_percent"]
          metrics_collection_interval = 300
        }
        mem = {
          measurement                 = ["mem_used_percent"]
          metrics_collection_interval = 300
        }
      }
    }
  })
}

# A full /data is the one host failure that breaks ClickHouse and corrupts
# in-flight dlt loads at the same time, and nothing inside the stack watches
# for it.
resource "aws_cloudwatch_metric_alarm" "data_disk" {
  count               = var.enable_cloudwatch_agent ? 1 : 0
  alarm_name          = "${var.project}-data-disk"
  namespace           = "CWAgent"
  metric_name         = "disk_used_percent"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.data_disk_alarm_threshold
  comparison_operator = "GreaterThanThreshold"
  # An agent that has stopped publishing is itself worth an email — with
  # notBreaching, a dead agent and a healthy disk look identical, and the alarm
  # stays green forever. Expect one alarm-then-OK cycle at first boot, in the
  # window before the agent's first datapoint; that is the alarm path proving
  # itself.
  treat_missing_data = "breaching"

  dimensions = {
    InstanceId = aws_instance.main.id
    path       = "/data"
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

# Early warning, before the kernel picks a victim — which on cgroup v2 means
# a whole container, not a process.
resource "aws_cloudwatch_metric_alarm" "memory" {
  count               = var.enable_cloudwatch_agent ? 1 : 0
  alarm_name          = "${var.project}-memory"
  namespace           = "CWAgent"
  metric_name         = "mem_used_percent"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = 92
  comparison_operator = "GreaterThanThreshold"
  # Same reasoning as the disk alarm: silence is not health.
  treat_missing_data = "breaching"

  dimensions = { InstanceId = aws_instance.main.id }

  alarm_actions = [aws_sns_topic.alarms.arn]
}
