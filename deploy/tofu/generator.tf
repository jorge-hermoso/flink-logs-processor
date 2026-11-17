# Optional: log-generator on ECS Fargate, feeding the input stream with synthetic plant logs.
# Enable with generator_enabled = true. Not meant for production environments.

locals {
  generator_count = var.generator_enabled ? 1 : 0
}

resource "aws_ecr_repository" "generator" {
  name                 = "${local.name}-log-generator"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.environment != "prod"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "generator" {
  repository = aws_ecr_repository.generator.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 10 most recent images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

# The VPC of the generator's security group is the VPC of its first subnet
data "aws_subnet" "generator" {
  count = var.generator_enabled && length(var.generator_subnet_ids) > 0 ? 1 : 0
  id    = var.generator_subnet_ids[0]
}

resource "aws_ecs_cluster" "generator" {
  count = local.generator_count
  name  = "${local.name}-generator"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  lifecycle {
    precondition {
      condition     = var.generator_image != "" && length(var.generator_subnet_ids) > 0
      error_message = "generator_enabled requires generator_image and generator_subnet_ids."
    }
  }
}

resource "aws_cloudwatch_log_group" "generator" {
  count             = local.generator_count
  name              = "/ecs/${local.name}-generator"
  retention_in_days = 7
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "generator_execution" {
  count              = local.generator_count
  name_prefix        = "${local.name}-gen-exec-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "generator_execution" {
  count      = local.generator_count
  role       = aws_iam_role.generator_execution[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "generator_task" {
  count              = local.generator_count
  name_prefix        = "${local.name}-gen-task-"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "generator_task" {
  statement {
    actions   = ["kinesis:PutRecords", "kinesis:PutRecord", "kinesis:DescribeStreamSummary"]
    resources = [aws_kinesis_stream.logs.arn]
  }
  statement {
    actions   = ["kms:GenerateDataKey"]
    resources = ["arn:${data.aws_partition.current.partition}:kms:${var.region}:${data.aws_caller_identity.current.account_id}:key/*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "generator_task" {
  count  = local.generator_count
  name   = "write-logs"
  role   = aws_iam_role.generator_task[0].id
  policy = data.aws_iam_policy_document.generator_task.json
}

resource "aws_security_group" "generator" {
  count       = local.generator_count
  name_prefix = "${local.name}-generator-"
  description = "log-generator: outbound HTTPS only"
  vpc_id      = one(data.aws_subnet.generator[*].vpc_id)

  egress {
    description = "Kinesis API"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_task_definition" "generator" {
  count                    = local.generator_count
  family                   = "${local.name}-generator"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.generator_execution[0].arn
  task_role_arn            = aws_iam_role.generator_task[0].arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "log-generator"
    image     = var.generator_image
    essential = true
    command = concat(
      ["--sink", "kinesis", "--stream", aws_kinesis_stream.logs.name, "--region", var.region],
      var.generator_args,
    )
    readonlyRootFilesystem = true
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.generator[0].name
        awslogs-region        = var.region
        awslogs-stream-prefix = "loggen"
      }
    }
  }])
}

resource "aws_ecs_service" "generator" {
  count           = local.generator_count
  name            = "log-generator"
  cluster         = aws_ecs_cluster.generator[0].id
  task_definition = aws_ecs_task_definition.generator[0].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.generator_subnet_ids
    security_groups  = [aws_security_group.generator[0].id]
    assign_public_ip = var.generator_assign_public_ip
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
}
