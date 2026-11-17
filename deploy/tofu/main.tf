locals {
  name = "${var.environment}-plant-anomaly"

  # Application properties, in the shape the job reads (see src/main/resources/flink-application-properties-dev.json).
  # Credentials are not configured: on the service the job uses the application's IAM role.
  property_groups = merge(
    {
      Source = {
        "stream.arn"           = aws_kinesis_stream.logs.arn
        "source.init.position" = "LATEST"
        "aws.region"           = var.region
      }
      Parser = {
        "max.line.length"          = "16384"
        "max.future.skew.ms"       = "300000"
        "dead.letter.unattributed" = "false"
      }
      Features = {
        "window.size.ms"          = tostring(var.window_size_ms)
        "max.out.of.orderness.ms" = "10000"
        "idle.timeout.ms"         = "60000"
      }
      RcfOperator = {
        "model.cache.max.size"  = "1000"
        "model.state.ttl.hours" = "0"
      }
      Alerting = {
        "min.grade"      = tostring(var.alert_min_grade)
        "cooldown.ms"    = tostring(var.alert_cooldown_ms)
        "warmup.windows" = tostring(var.alert_warmup_windows)
      }
      Rules = {
        "silence.after.ms" = tostring(var.silence_after_ms)
        "stuck.windows"    = tostring(var.stuck_windows)
      }
      AlertStream = {
        "stream.name" = aws_kinesis_stream.alerts.name
        "aws.region"  = var.region
      }
      DeadLetterStream = {
        "stream.name" = aws_kinesis_stream.dead_letters.name
        "aws.region"  = var.region
      }
      ScoresTable = {
        "table.name" = aws_dynamodb_table.scores.name
        "ttl.days"   = tostring(var.scores_ttl_days)
        "aws.region" = var.region
      }
    },
    { for type, metrics in var.station_profiles : "Station-${upper(type)}" => { for name, label in metrics : "metric.${name}" => label } },
    { for key, params in var.model_parameters : "ModelParameters-${key}" => params },
  )
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
