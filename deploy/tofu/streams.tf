# Input: raw log lines. Retention is the catch-up window when the job is stopped or failing.
resource "aws_kinesis_stream" "logs" {
  name             = "${local.name}-logs"
  shard_count      = var.log_stream_shards > 0 ? var.log_stream_shards : null
  retention_period = var.log_retention_hours
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = var.log_stream_shards > 0 ? "PROVISIONED" : "ON_DEMAND"
  }

  shard_level_metrics = ["IncomingRecords", "IteratorAgeMilliseconds"]
}

# Output: anomaly alerts (JSON), consumed by the notification pipe and any other subscriber.
resource "aws_kinesis_stream" "alerts" {
  name             = "${local.name}-alerts"
  retention_period = 168
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# Output: records the job could not process. Kept a week for investigation and replay.
resource "aws_kinesis_stream" "dead_letters" {
  name             = "${local.name}-dead-letters"
  retention_period = 168
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# Output: every scored window, for dashboards and history. Key matches ScoredRecordConverter.
resource "aws_dynamodb_table" "scores" {
  name         = "${local.name}-scores"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "station_id"
  range_key    = "window_end"

  attribute {
    name = "station_id"
    type = "S"
  }

  attribute {
    name = "window_end"
    type = "N"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = var.scores_ttl_days > 0
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  deletion_protection_enabled = var.environment == "prod"
}
