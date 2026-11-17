data "aws_iam_policy_document" "flink_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "flink" {
  name_prefix        = "${local.name}-flink-"
  assume_role_policy = data.aws_iam_policy_document.flink_assume.json
}

# Least privilege: read the code, read the input, write the three outputs, write logs.
data "aws_iam_policy_document" "flink" {
  statement {
    sid       = "ReadApplicationCode"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.artifacts.arn}/application/*"]
  }

  statement {
    sid = "ReadLogStream"
    actions = [
      "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:DescribeStreamConsumer",
      "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards", "kinesis:SubscribeToShard",
      "kinesis:RegisterStreamConsumer", "kinesis:DeregisterStreamConsumer", "kinesis:ListStreamConsumers",
    ]
    resources = [aws_kinesis_stream.logs.arn, "${aws_kinesis_stream.logs.arn}/consumer/*"]
  }

  statement {
    sid       = "WriteOutputStreams"
    actions   = ["kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.alerts.arn, aws_kinesis_stream.dead_letters.arn]
  }

  statement {
    sid       = "UseKinesisManagedKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["arn:${data.aws_partition.current.partition}:kms:${var.region}:${data.aws_caller_identity.current.account_id}:key/*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "WriteScores"
    actions   = ["dynamodb:BatchWriteItem", "dynamodb:PutItem", "dynamodb:DescribeTable"]
    resources = [aws_dynamodb_table.scores.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:PutLogEvents", "logs:DescribeLogStreams", "logs:DescribeLogGroups"]
    resources = [aws_cloudwatch_log_group.application.arn, "${aws_cloudwatch_log_group.application.arn}:*"]
  }

  statement {
    sid       = "PublishMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "flink" {
  name   = "detector"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}
