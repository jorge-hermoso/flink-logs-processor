#!/bin/sh
# Creates the streams and the table named in flink-application-properties-dev.json
set -e
awslocal kinesis create-stream --stream-name plant-logs --shard-count 2
awslocal kinesis create-stream --stream-name plant-anomalies --shard-count 1
awslocal kinesis create-stream --stream-name plant-dead-letters --shard-count 1
awslocal dynamodb create-table --table-name plant-scores \
  --attribute-definitions AttributeName=station_id,AttributeType=S AttributeName=window_end,AttributeType=N \
  --key-schema AttributeName=station_id,KeyType=HASH AttributeName=window_end,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
