variable "region" {
  description = "AWS region to deploy to."
  type        = string
}

variable "environment" {
  description = "Environment name, used as a prefix for every resource (e.g. dev, prod)."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be 2-16 lower-case letters, digits or dashes, starting with a letter."
  }
}

variable "tags" {
  description = "Extra tags applied to every resource."
  type        = map(string)
  default     = {}
}

# --- Application artifact -------------------------------------------------------------------------

variable "application_jar_path" {
  description = "Local path of the shaded application JAR built by `mvn package`."
  type        = string
  default     = "../../target/flink_logs_processor_2.3.0-1.0.jar"
}

variable "flink_runtime" {
  description = "Managed Service for Apache Flink runtime. Must match the Flink version in pom.xml."
  type        = string
  default     = "FLINK-2_3"
}

# --- Capacity -------------------------------------------------------------------------------------

variable "log_stream_shards" {
  description = "Shards of the input log stream. 0 uses on-demand capacity mode."
  type        = number
  default     = 0
}

variable "parallelism" {
  description = "Initial parallelism of the Flink job."
  type        = number
  default     = 2
}

variable "parallelism_per_kpu" {
  description = "Parallel tasks per KPU (1 KPU = 1 vCPU, 4 GB). The RCF models are CPU and memory heavy: keep at 1."
  type        = number
  default     = 1
}

variable "autoscaling_enabled" {
  description = "Let the service scale parallelism with CPU usage. Rescaling restarts the job from a snapshot."
  type        = bool
  default     = true
}

variable "checkpoint_interval_ms" {
  description = "Checkpoint interval. Bounds how much input is reprocessed after a failure."
  type        = number
  default     = 60000
}

variable "start_application" {
  description = "Start the application after deployment."
  type        = bool
  default     = true
}

variable "restore_from_snapshot" {
  description = "Restore from the latest snapshot on start (keeps the trained models). Set false only to deliberately reset all models."
  type        = bool
  default     = true
}

variable "allow_non_restored_state" {
  description = "Allow starting from a snapshot containing state for operators that no longer exist."
  type        = bool
  default     = false
}

# --- Data retention -------------------------------------------------------------------------------

variable "log_retention_hours" {
  description = "Retention of the input log stream. It is the window in which a stopped job can catch up without data loss."
  type        = number
  default     = 72
}

variable "scores_ttl_days" {
  description = "Days scored windows are kept in DynamoDB. 0 keeps them for ever."
  type        = number
  default     = 30
}

variable "cloudwatch_log_retention_days" {
  description = "Retention of the application's CloudWatch logs."
  type        = number
  default     = 30
}

# --- Detection ------------------------------------------------------------------------------------

variable "station_profiles" {
  description = <<-EOT
    Metrics to read from each station type's logs: { TYPE = { metric_name = "label in the log text" } }.
    The default matches the synthetic plant of log-generator/.
    Changing the metrics of a type resets the models of its stations.
  EOT
  type        = map(map(string))
  default = {
    MIXER    = { viscosity = "visc", slurry_temp = "temp", agitator_rpm = "agitator_rpm" }
    EXTRUDER = { melt_pressure = "melt pressure", screw_torque = "screw torque", barrel_temp = "barrel temp" }
    KILN     = { zone_temp = "zone temp reading", o2 = "o2", belt_speed = "belt" }
    PRESS    = { peak_force = "peak_force", cycle_time = "cycle_time", hydraulic_pressure = "hyd_press" }
    PACKER   = { rate = "rate", rejects = "rejects", vacuum = "vacuum" }
  }
}

variable "model_parameters" {
  description = "Random Cut Forest parameters: DEFAULT plus optional overrides keyed by station type or station id. See docs/CONFIGURATION.md."
  type        = map(map(string))
  default = {
    DEFAULT = {
      "shingle.size"    = "6"
      "number.of.trees" = "50"
      "sample.size"     = "256"
      "output.after"    = "64"
      "anomaly.rate"    = "0.005"
    }
  }

  validation {
    condition     = contains(keys(var.model_parameters), "DEFAULT")
    error_message = "model_parameters must contain a DEFAULT entry."
  }
}

variable "window_size_ms" {
  description = "Length of the feature windows."
  type        = number
  default     = 10000
}

variable "alert_min_grade" {
  description = "Minimum anomaly grade (0-1) that produces an alert. Lower grades are still stored in the scores table."
  type        = number
  default     = 0.5

  validation {
    condition     = var.alert_min_grade >= 0 && var.alert_min_grade <= 1
    error_message = "alert_min_grade must be between 0 and 1."
  }
}

variable "alert_warmup_windows" {
  description = "Windows a new model must score before its anomalies are alerted (its threshold is still settling)."
  type        = number
  default     = 60
}

variable "silence_after_ms" {
  description = "Event time without any log line before a station raises a SILENCE alert. 0 disables the rule."
  type        = number
  default     = 60000
}

variable "stuck_windows" {
  description = "Consecutive identical windows before a metric raises a STUCK_SENSOR alert. 0 disables the rule."
  type        = number
  default     = 12
}

variable "alert_cooldown_ms" {
  description = "Minimum time between two alerts for the same station."
  type        = number
  default     = 300000
}

# --- Notifications --------------------------------------------------------------------------------

variable "anomaly_email_subscribers" {
  description = "E-mail addresses notified of anomalies. Each must confirm the subscription."
  type        = list(string)
  default     = []
}

variable "ops_email_subscribers" {
  description = "E-mail addresses notified when the detector itself is unhealthy."
  type        = list(string)
  default     = []
}

variable "dead_letter_alarm_threshold" {
  description = "Dead-lettered records per 5 minutes that raise an operational alarm."
  type        = number
  default     = 100
}

# --- Synthetic data generator (optional) ----------------------------------------------------------

variable "generator_enabled" {
  description = "Run log-generator on ECS Fargate, writing to the input stream. For demos and non-production environments."
  type        = bool
  default     = false
}

variable "generator_image" {
  description = "Container image of log-generator. Required when generator_enabled; push it to the ECR repository this module creates (see outputs)."
  type        = string
  default     = ""
}

variable "generator_subnet_ids" {
  description = "Subnets for the generator task. Need a route to the Kinesis endpoint (NAT, VPC endpoint or public IP)."
  type        = list(string)
  default     = []
}

variable "generator_assign_public_ip" {
  description = "Give the generator task a public IP (for public subnets without NAT)."
  type        = bool
  default     = false
}

variable "generator_args" {
  description = "Extra arguments for log-generator (the sink, stream and region are set by this module)."
  type        = list(string)
  default     = ["--backfill", "3600", "--anomaly-rate", "1", "--lines-per-record", "10"]
}
