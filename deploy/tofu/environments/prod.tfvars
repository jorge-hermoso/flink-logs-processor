region      = "eu-west-1"
environment = "prod"

parallelism         = 2
autoscaling_enabled = true
log_retention_hours = 168

alert_cooldown_ms = 600000

anomaly_email_subscribers = [] # ["line-supervisors@example.com"]
ops_email_subscribers     = [] # ["platform-oncall@example.com"]
