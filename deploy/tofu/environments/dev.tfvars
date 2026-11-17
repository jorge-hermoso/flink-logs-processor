region      = "eu-west-1"
environment = "dev"

parallelism         = 1
autoscaling_enabled = false
log_retention_hours = 24

# Feed the dev environment with synthetic logs (see docs/DEPLOYMENT.md to build and push the image)
generator_enabled          = false
generator_image            = "" # "<account>.dkr.ecr.eu-west-1.amazonaws.com/dev-plant-anomaly-log-generator:1.0.0"
generator_subnet_ids       = []
generator_assign_public_ip = true

anomaly_email_subscribers = []
ops_email_subscribers     = []
