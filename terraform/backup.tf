# Snapshots of the data volume are the backup, and they are crash-consistent
# by design rather than quiesced.
#
# A snapshot is point-in-time for the whole volume — the equivalent of pulling
# the power. ClickHouse MergeTree writes parts to temporary names and renames
# atomically, both Postgres instances recover through their WAL, and dlt
# drains any pending load package on the next run. Nothing here needs the
# stack stopped.
#
# What matters far more is that everything lives on ONE volume. warehouse-data
# holds the rows and dlt-state holds the cursor describing them; because both
# are Docker volumes under a data-root on this single disk, one snapshot
# captures them at the same instant. That is why the restore rule is: restore
# the whole volume, never a directory out of it.
#
# For the moments where crash-consistent is not good enough — before a
# Metabase or Airflow upgrade — take a cold one by hand, which costs about a
# minute of downtime:
#   docker compose stop && aws ec2 create-snapshot --volume-id <id> && docker compose start

resource "aws_dlm_lifecycle_policy" "data" {
  description        = "${var.project} data volume"
  execution_role_arn = aws_iam_role.dlm.arn
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]
    target_tags = {
      Backup = var.project
    }

    schedule {
      name = "daily"

      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        # Quiet hour for this stack: the hourly ingest runs at :17 and the
        # weekly reconcile starts at 03:00 Saturday.
        times = ["09:00"]
      }

      retain_rule {
        count = var.snapshot_daily_retain
      }

      tags_to_add = {
        SnapshotCreator = "dlm"
        Schedule        = "daily"
      }

      copy_tags = true
    }

    schedule {
      name = "weekly"

      create_rule {
        cron_expression = "cron(0 9 ? * SUN *)"
      }

      retain_rule {
        count = var.snapshot_weekly_retain
      }

      tags_to_add = {
        SnapshotCreator = "dlm"
        Schedule        = "weekly"
      }

      copy_tags = true
    }
  }
}
