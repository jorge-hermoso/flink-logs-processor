output "application_name" {
  description = "Managed Service for Apache Flink application name."
  value       = aws_kinesisanalyticsv2_application.detector.name
}

output "log_stream_name" {
  description = "Stream to send raw log lines to."
  value       = aws_kinesis_stream.logs.name
}

output "log_stream_arn" {
  value = aws_kinesis_stream.logs.arn
}

output "alert_stream_name" {
  description = "Stream with anomaly alerts (JSON)."
  value       = aws_kinesis_stream.alerts.name
}

output "dead_letter_stream_name" {
  description = "Stream with records the job could not process (JSON)."
  value       = aws_kinesis_stream.dead_letters.name
}

output "scores_table_name" {
  description = "DynamoDB table with every scored window."
  value       = aws_dynamodb_table.scores.name
}

output "anomaly_topic_arn" {
  description = "SNS topic receiving anomaly notifications; subscribe further endpoints to it."
  value       = aws_sns_topic.anomalies.arn
}

output "ops_topic_arn" {
  description = "SNS topic receiving the detector's health alarms."
  value       = aws_sns_topic.ops.arn
}

output "generator_repository_url" {
  description = "ECR repository for the log-generator image."
  value       = aws_ecr_repository.generator.repository_url
}

output "dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.detector.dashboard_name}"
}
