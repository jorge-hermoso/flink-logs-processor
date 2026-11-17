# Deployment (OpenTofu)

`deploy/tofu/` deploys the detector and everything around it into one AWS account and region.

## What gets created

| Resource | Name | Purpose |
|---|---|---|
| Kinesis stream | `<env>-plant-anomaly-logs` | Input: raw log lines. On-demand by default, or `log_stream_shards` provisioned shards. KMS-encrypted. |
| Kinesis stream | `<env>-plant-anomaly-alerts` | Output: anomaly alerts (JSON), 7-day retention. |
| Kinesis stream | `<env>-plant-anomaly-dead-letters` | Output: unprocessable records, 7-day retention. |
| DynamoDB table | `<env>-plant-anomaly-scores` | Output: every scored window. On-demand, TTL on `expires_at`, point-in-time recovery, deletion protection in `prod`. |
| S3 bucket | `<env>-plant-anomaly-artifacts-*` | Application JARs, versioned, private, encrypted. Each build is a new key, so earlier builds stay available for rollback. |
| Managed Flink application | `<env>-plant-anomaly` | The job. Snapshots on, restore from latest snapshot, checkpoints every 60 s, autoscaling, CloudWatch logging. |
| IAM roles | `<env>-plant-anomaly-flink-*`, `-pipe-*` | Least privilege: each role can only touch its own streams, table, bucket prefix and log group. |
| EventBridge Pipe | `<env>-plant-anomaly-alerts` | Alerts stream → SNS, reshaped into a readable message. No code to maintain. |
| SNS topics | `-anomalies`, `-ops` | Anomaly notifications; detector health alarms (customer-managed KMS key so CloudWatch can publish). |
| CloudWatch | alarms + dashboard `<env>-plant-anomaly` | See [OPERATIONS.md](OPERATIONS.md). |
| ECR repository | `<env>-plant-anomaly-log-generator` | Image of the synthetic log generator. |
| ECS Fargate service | *(optional)* | Runs the log generator against the input stream (`generator_enabled`). |

## Prerequisites

* OpenTofu ≥ 1.8 (tested with 1.10), AWS credentials for the target account.
* JDK 17 and Maven 3.9 to build the JAR. **Java 17, not 21**: Managed Service for Apache Flink
  does not support Java 21.
* For the generator: Docker.

## First deployment

```bash
mvn clean package                      # builds target/flink_logs_processor_2.3.0-1.0.jar
cd deploy/tofu
tofu init                              # local state; see "Remote state" below for shared environments
tofu plan  -var-file=environments/dev.tfvars -out=dev.plan
tofu apply dev.plan
```

The application starts automatically (`start_application = true`). The first ~20 minutes it
learns: models score after ~11 min, alerts start ~10 min later (see
[CONFIGURATION.md](CONFIGURATION.md#defaults-in-practice)).

Send logs to the `log_stream_name` output. Each record can hold one line, or several lines
separated by `\n`. Use a per-station partition key so each station's lines stay in order.

Subscribers in `anomaly_email_subscribers` / `ops_email_subscribers` receive a confirmation
e-mail from SNS and must accept it.

## Feeding a non-production environment with synthetic logs

```bash
REPO=$(tofu output -raw generator_repository_url)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker build -t "$REPO:1.0.0" ../../log-generator && docker push "$REPO:1.0.0"
```

Then in the tfvars, set `generator_enabled = true`, `generator_image = "<REPO>:1.0.0"` and
`generator_subnet_ids` to subnets that can reach Kinesis (public subnets with
`generator_assign_public_ip = true`, or private subnets with NAT or a Kinesis VPC endpoint), and
apply again. By default it backfills one hour of clean history, so models are trained quickly,
then runs live with about one random anomaly per station per hour (`generator_args`). The
generator's log shows the seed; the anomalies are reproducible from it.

## Updating the application

Rebuild the JAR and apply. The S3 key contains the JAR's hash, so tofu sees a new code location
and updates the application. The service takes a snapshot, restarts the job with the new code
and restores the snapshot, so trained models are kept.

Before changing the job, check what restores:

* **Operator `uid`s are part of the state contract.** Renaming one (`parse`, `features`, `rcf`,
  `alerts`, `kinesis-source`, sinks) loses that operator's state. The restore then fails, unless
  `allow_non_restored_state = true`, which accepts the loss.
* Changing a station type's metrics resets that type's models (by design; see CONFIGURATION.md).
* Changing `window.size.ms` changes what the models have learned. Treat it like a reset.
  Deploy with `restore_from_snapshot = false` to start over cleanly.

## Rolling back

Previous JARs stay in the versioned bucket. To roll back, rebuild or check out the previous
version and apply, or roll back from the console/CLI (`aws kinesisanalyticsv2 rollback-application`),
which also reverts to the snapshot taken before the update. Then align tofu with the code
running in AWS before the next apply.

## Upgrading Flink

Change `flink.version` and the connector versions in `pom.xml`, and `flink_runtime` here, in
the same change. In-place upgrades keep the snapshot. Models are stored in a library-independent
byte format (see ARCHITECTURE.md), so a Flink upgrade does not affect them. Test the restore with a
snapshot from the current version before upgrading production.

## Remote state

Shared environments need remote state with locking:

1. Copy `environments/prod.s3.tfbackend.example` to `environments/prod.s3.tfbackend` and fill it in.
2. Uncomment `backend "s3" {}` in `versions.tf`.
3. `tofu init -backend-config=environments/prod.s3.tfbackend`.

## Destroying

`tofu destroy -var-file=...`. In `prod`, the scores table has deletion protection and the
artifact bucket is not force-destroyed: both have to be handled by hand on purpose.

## Testing the template

`deploy/tofu/tests/plan.tftest.hcl` plans and "applies" the whole stack against a mocked AWS
provider, with no account needed. It checks the rendered application properties, the
on-demand/provisioned stream switch, the optional generator and input validation:

```bash
cd deploy/tofu && tofu init -backend=false && tofu test     # needs the JAR built first
```

What the mock cannot check (IAM policy sufficiency, the Pipe's input template, service quotas)
is only verified by a real deployment. See the open tasks in [TASKS.md](TASKS.md).
