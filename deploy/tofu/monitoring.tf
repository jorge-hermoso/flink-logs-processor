# Health of the detector itself. Every alarm notifies the ops topic, on alarm and on recovery.

locals {
  app_dimensions = { Application = aws_kinesisanalyticsv2_application.detector.name }
  alarm_actions  = [aws_sns_topic.ops.arn]
}

# The job is not running (failing, restarting or stopped by mistake).
resource "aws_cloudwatch_metric_alarm" "downtime" {
  alarm_name          = "${local.name}-downtime"
  alarm_description   = "The detector job is not running. Check the application logs for the failure cause."
  namespace           = "AWS/KinesisAnalytics"
  metric_name         = "downtime"
  dimensions          = local.app_dimensions
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# The job keeps restarting: a crash loop it will not recover from by itself.
resource "aws_cloudwatch_metric_alarm" "restarts" {
  alarm_name          = "${local.name}-restarts"
  alarm_description   = "The detector job restarted more than twice in 15 minutes."
  evaluation_periods  = 3
  threshold           = 2
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions

  metric_query {
    id          = "restarts"
    expression  = "DIFF(m1)"
    label       = "Restarts per 5 minutes"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      namespace   = "AWS/KinesisAnalytics"
      metric_name = "fullRestarts"
      dimensions  = local.app_dimensions
      period      = 300
      stat        = "Maximum"
    }
  }
}

# Checkpoints are failing: a restart now would reprocess a lot, and snapshots may fail too.
resource "aws_cloudwatch_metric_alarm" "failed_checkpoints" {
  alarm_name          = "${local.name}-failed-checkpoints"
  alarm_description   = "Checkpoints of the detector job are failing."
  evaluation_periods  = 3
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions

  metric_query {
    id          = "failed"
    expression  = "DIFF(m1)"
    label       = "Failed checkpoints per 5 minutes"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      namespace   = "AWS/KinesisAnalytics"
      metric_name = "numberOfFailedCheckpoints"
      dimensions  = local.app_dimensions
      period      = 300
      stat        = "Maximum"
    }
  }
}

# The job is falling behind the logs: detection is delayed, and data expires if it lasts.
resource "aws_cloudwatch_metric_alarm" "lag" {
  alarm_name          = "${local.name}-lag"
  alarm_description   = "The detector is more than 10 minutes behind the input logs."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = aws_kinesis_stream.logs.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 600000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# Running but producing nothing: no logs arriving, or every line rejected.
resource "aws_cloudwatch_metric_alarm" "no_scores" {
  alarm_name          = "${local.name}-no-scores"
  alarm_description   = "No feature window scored in 15 minutes: logs stopped arriving or none can be parsed."
  namespace           = "AWS/KinesisAnalytics"
  metric_name         = "scoredWindows"
  dimensions          = local.app_dimensions
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# Many unprocessable records: usually a log format change that the station profiles do not match.
resource "aws_cloudwatch_metric_alarm" "dead_letters" {
  alarm_name          = "${local.name}-dead-letters"
  alarm_description   = "Unusual number of dead-lettered records. Inspect the dead-letter stream."
  namespace           = "AWS/Kinesis"
  metric_name         = "IncomingRecords"
  dimensions          = { StreamName = aws_kinesis_stream.dead_letters.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.dead_letter_alarm_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# The notification pipe cannot deliver alerts.
resource "aws_cloudwatch_metric_alarm" "pipe_failures" {
  alarm_name          = "${local.name}-alert-delivery"
  alarm_description   = "The alert pipe failed to publish anomaly notifications to SNS."
  namespace           = "AWS/EventBridge/Pipes"
  metric_name         = "ExecutionFailed"
  dimensions          = { PipeName = aws_pipes_pipe.alerts_to_sns.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_dashboard" "detector" {
  dashboard_name = local.name
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title = "Throughput", region = var.region, stat = "Sum", period = 300, view = "timeSeries"
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", aws_kinesis_stream.logs.name, { label = "Log records in" }],
            ["AWS/KinesisAnalytics", "parsedRecords", "Application", local.name, { label = "Lines parsed" }],
            [".", "unattributedRecords", ".", ".", { label = "Lines without station" }],
            [".", "deadLetterRecords", ".", ".", { label = "Dead letters" }],
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6
        properties = {
          title = "Detection", region = var.region, stat = "Sum", period = 300, view = "timeSeries"
          metrics = [
            ["AWS/KinesisAnalytics", "scoredWindows", "Application", local.name, { label = "Windows scored" }],
            [".", "anomalies", ".", ".", { label = "Anomalous windows" }],
            [".", "alertsEmitted", ".", ".", { label = "Alerts sent" }],
            [".", "alertsSuppressed", ".", ".", { label = "Alerts suppressed (cool-down)" }],
            [".", "modelResets", ".", ".", { label = "Model resets" }],
          ]
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 8, height = 6
        properties = {
          title   = "Lag behind the logs (ms)", region = var.region, stat = "Maximum", period = 60, view = "timeSeries"
          metrics = [["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", aws_kinesis_stream.logs.name]]
        }
      },
      {
        type = "metric", x = 8, y = 6, width = 8, height = 6
        properties = {
          title = "Checkpoints", region = var.region, stat = "Maximum", period = 60, view = "timeSeries"
          metrics = [
            ["AWS/KinesisAnalytics", "lastCheckpointDuration", "Application", local.name, { label = "Duration (ms)" }],
            [".", "lastCheckpointSize", ".", ".", { label = "Size (bytes)", yAxis = "right" }],
          ]
        }
      },
      {
        type = "metric", x = 16, y = 6, width = 8, height = 6
        properties = {
          title = "Resources", region = var.region, stat = "Average", period = 60, view = "timeSeries"
          metrics = [
            ["AWS/KinesisAnalytics", "cpuUtilization", "Application", local.name, { label = "CPU %" }],
            [".", "heapMemoryUtilization", ".", ".", { label = "Heap %" }],
            [".", "KPUs", ".", ".", { label = "KPUs", yAxis = "right", stat = "Maximum" }],
          ]
        }
      },
      {
        type = "alarm", x = 0, y = 12, width = 24, height = 3
        properties = {
          title = "Alarms"
          alarms = [
            aws_cloudwatch_metric_alarm.downtime.arn, aws_cloudwatch_metric_alarm.restarts.arn,
            aws_cloudwatch_metric_alarm.failed_checkpoints.arn, aws_cloudwatch_metric_alarm.lag.arn,
            aws_cloudwatch_metric_alarm.no_scores.arn, aws_cloudwatch_metric_alarm.dead_letters.arn,
            aws_cloudwatch_metric_alarm.pipe_failures.arn,
          ]
        }
      },
    ]
  })
}
