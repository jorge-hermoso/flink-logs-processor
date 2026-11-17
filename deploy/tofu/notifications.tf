# Anomaly alerts: alert stream -> EventBridge Pipe -> SNS topic -> subscribers.
# The pipe needs no code; it turns each alert into a readable notification.

resource "aws_sns_topic" "anomalies" {
  name              = "${local.name}-anomalies"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "anomalies_email" {
  for_each  = toset(var.anomaly_email_subscribers)
  topic_arn = aws_sns_topic.anomalies.arn
  protocol  = "email"
  endpoint  = each.value
}

# Detector health: CloudWatch alarms publish here.
resource "aws_sns_topic" "ops" {
  name              = "${local.name}-ops"
  kms_master_key_id = aws_kms_key.ops.arn
}

resource "aws_sns_topic_subscription" "ops_email" {
  for_each  = toset(var.ops_email_subscribers)
  topic_arn = aws_sns_topic.ops.arn
  protocol  = "email"
  endpoint  = each.value
}

# CloudWatch alarms can only publish to an encrypted topic whose key policy allows them, which the
# AWS-managed SNS key does not: the ops topic uses its own customer-managed key.
resource "aws_kms_key" "ops" {
  description         = "${local.name} ops notifications"
  enable_key_rotation = true
  policy              = data.aws_iam_policy_document.ops_key.json
}

data "aws_iam_policy_document" "ops_key" {
  statement {
    sid       = "AccountAdmin"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
  statement {
    sid       = "CloudWatchAlarms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "pipe_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["pipes.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "pipe" {
  name_prefix        = "${local.name}-pipe-"
  assume_role_policy = data.aws_iam_policy_document.pipe_assume.json
}

data "aws_iam_policy_document" "pipe" {
  statement {
    actions   = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards", "kinesis:ListStreams", "kinesis:SubscribeToShard"]
    resources = [aws_kinesis_stream.alerts.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.anomalies.arn]
  }
  statement {
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["arn:${data.aws_partition.current.partition}:kms:${var.region}:${data.aws_caller_identity.current.account_id}:key/*"]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.region}.amazonaws.com", "sns.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "pipe" {
  name   = "alerts-to-sns"
  role   = aws_iam_role.pipe.id
  policy = data.aws_iam_policy_document.pipe.json
}

resource "aws_pipes_pipe" "alerts_to_sns" {
  name     = "${local.name}-alerts"
  role_arn = aws_iam_role.pipe.arn
  source   = aws_kinesis_stream.alerts.arn
  target   = aws_sns_topic.anomalies.arn

  source_parameters {
    kinesis_stream_parameters {
      starting_position      = "LATEST"
      batch_size             = 1
      maximum_retry_attempts = 10
    }
  }

  # Kinesis data is decoded and parsed as JSON by the pipe (implicit body parsing)
  target_parameters {
    input_template = <<-EOT
      {"subject": "<$.data.type> on <$.data.stationId>", "type": <$.data.type>, "summary": <$.data.summary>, "detectedAt": <$.data.detectedAt>, "grade": <$.data.grade>, "alertId": <$.data.alertId>, "alert": <$.data>}
    EOT
  }

  depends_on = [aws_iam_role_policy.pipe]
}
