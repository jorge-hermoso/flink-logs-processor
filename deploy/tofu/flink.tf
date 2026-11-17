# --- Code artifact --------------------------------------------------------------------------------

resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${local.name}-artifacts-"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "expire-old-jars"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days           = 90
      newer_noncurrent_versions = 5
    }
  }
}

# The object key includes the content hash, so every new build is a new key: the application
# notices the change and redeploys, and the previous JAR stays available for rollback.
resource "aws_s3_object" "application_jar" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "application/${filemd5(var.application_jar_path)}/${basename(var.application_jar_path)}"
  source = var.application_jar_path
  etag   = filemd5(var.application_jar_path)
}

# --- Logging --------------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "application" {
  name              = "/aws/kinesis-analytics/${local.name}"
  retention_in_days = var.cloudwatch_log_retention_days
}

resource "aws_cloudwatch_log_stream" "application" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.application.name
}

# --- Application ----------------------------------------------------------------------------------

resource "aws_kinesisanalyticsv2_application" "detector" {
  name                   = local.name
  description            = "Production logs anomaly detector (Random Cut Forest)"
  runtime_environment    = var.flink_runtime
  service_execution_role = aws_iam_role.flink.arn
  start_application      = var.start_application

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.artifacts.arn
          file_key       = aws_s3_object.application_jar.key
          object_version = aws_s3_object.application_jar.version_id
        }
      }
    }

    # Snapshots are taken on every stop and update: they carry the trained models across deployments
    application_snapshot_configuration {
      snapshots_enabled = true
    }

    run_configuration {
      application_restore_configuration {
        application_restore_type = var.restore_from_snapshot ? "RESTORE_FROM_LATEST_SNAPSHOT" : "SKIP_RESTORE_FROM_SNAPSHOT"
      }
      flink_run_configuration {
        allow_non_restored_state = var.allow_non_restored_state
      }
    }

    environment_properties {
      dynamic "property_group" {
        for_each = local.property_groups
        content {
          property_group_id = property_group.key
          property_map      = property_group.value
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = 5000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        # APPLICATION aggregates the job's custom counters (see Metrics.java) per application
        metrics_level = "APPLICATION"
        log_level     = "INFO"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.parallelism
        parallelism_per_kpu  = var.parallelism_per_kpu
        auto_scaling_enabled = var.autoscaling_enabled
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.application.arn
  }

  # The role's policies must exist before the service validates access on create/start
  depends_on = [aws_iam_role_policy.flink]
}
