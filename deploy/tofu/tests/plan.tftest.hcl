# Plans the whole stack against a mocked AWS provider: no credentials or account needed.
# Run from deploy/tofu after building the JAR: tofu init -backend=false && tofu test

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_subnet" {
    defaults = { vpc_id = "vpc-0123456789abcdef0" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_kinesis_stream" {
    defaults = { arn = "arn:aws:kinesis:eu-west-1:123456789012:stream/mock" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::mock-bucket" }
  }
  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:eu-west-1:123456789012:mock" }
  }
  mock_resource "aws_kms_key" {
    defaults = { arn = "arn:aws:kms:eu-west-1:123456789012:key/mock" }
  }
  mock_resource "aws_cloudwatch_log_stream" {
    defaults = { arn = "arn:aws:logs:eu-west-1:123456789012:log-group:mock:log-stream:application" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:eu-west-1:123456789012:log-group:mock" }
  }
  mock_resource "aws_dynamodb_table" {
    defaults = { arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/mock" }
  }
  mock_resource "aws_cloudwatch_metric_alarm" {
    defaults = { arn = "arn:aws:cloudwatch:eu-west-1:123456789012:alarm:mock" }
  }
  mock_resource "aws_ecs_cluster" {
    defaults = { arn = "arn:aws:ecs:eu-west-1:123456789012:cluster/mock" }
  }
}

variables {
  region      = "eu-west-1"
  environment = "test"
}

run "default_stack" {
  command = apply

  assert {
    condition     = aws_kinesisanalyticsv2_application.detector.runtime_environment == "FLINK-2_3"
    error_message = "Runtime must match the Flink version of pom.xml."
  }

  assert {
    condition = alltrue([for group in ["Source", "AlertStream", "DeadLetterStream", "ScoresTable", "Station-KILN", "ModelParameters-DEFAULT"] :
    contains(keys(local.property_groups), group)])
    error_message = "A property group the job requires is missing."
  }

  assert {
    condition     = local.property_groups["Station-KILN"]["metric.zone_temp"] == "zone temp reading"
    error_message = "Station profiles are not rendered as metric.<name> properties."
  }

  assert {
    condition     = aws_kinesis_stream.logs.stream_mode_details[0].stream_mode == "ON_DEMAND"
    error_message = "log_stream_shards = 0 must give an on-demand stream."
  }

  assert {
    condition     = length(aws_ecs_service.generator) == 0
    error_message = "The generator must be off by default."
  }
}

run "with_generator_and_provisioned_stream" {
  command = apply

  variables {
    log_stream_shards    = 4
    generator_enabled    = true
    generator_image      = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/loggen:1.0.0"
    generator_subnet_ids = ["subnet-0123456789abcdef0"]
  }

  assert {
    condition     = aws_kinesis_stream.logs.shard_count == 4
    error_message = "Provisioned shard count not applied."
  }

  assert {
    condition     = length(aws_ecs_service.generator) == 1
    error_message = "Generator service not created."
  }
}

run "generator_needs_an_image" {
  command = plan

  variables {
    generator_enabled = true
  }

  expect_failures = [aws_ecs_cluster.generator]
}

run "rejects_bad_environment_name" {
  command = plan

  variables {
    environment = "Prod_Env"
  }

  expect_failures = [var.environment]
}
