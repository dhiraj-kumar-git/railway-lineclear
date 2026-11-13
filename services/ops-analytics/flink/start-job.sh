#!/bin/bash
# Starts the operations analytics Flink job on the EMR cluster. Runs as an EMR step on the
# primary node: starts a Flink session on YARN if none is running, then submits the SQL job
# through the Flink SQL client unless the job is already running, so running it twice is safe.
# Usage: start-job.sh <analytics bucket>

set -euo pipefail
BUCKET="$1"
SESSION_NAME=lineclear-analytics
WORK=/tmp/ops-analytics
FLINK=${FLINK_BIN:-/usr/lib/flink/bin}
mkdir -p "$WORK"
aws s3 cp --recursive --only-show-errors "s3://$BUCKET/flink/" "$WORK/"
chmod +x "$WORK"/*.sh

# The broker addresses change on every rebuild, so they are read from Secrets Manager here
CONNECTION=$(aws secretsmanager get-secret-value --region ap-south-1 --secret-id lineclear/kafka/connection --query SecretString --output text)
BOOTSTRAP=$(python3 -c 'import json, sys; print(json.loads(sys.argv[1])["bootstrap_servers"])' "$CONNECTION")

session_id() {
  yarn application -list -appStates RUNNING 2>/dev/null | awk -v name="$SESSION_NAME" '$2 == name {print $1}' | head -n 1
}

APP=$(session_id)

if [ -z "$APP" ]; then
  echo "Starting the Flink session $SESSION_NAME on YARN"
  "$FLINK/yarn-session.sh" -d -nm "$SESSION_NAME" -jm 1024m -tm 2048m -s 2
  for _ in $(seq 1 36); do
    APP=$(session_id)
    [ -n "$APP" ] && break
    sleep 5
  done
fi

if [ -z "$APP" ]; then
  echo "The Flink session did not start; see the YARN logs in the EMR console" >&2
  exit 1
fi
echo "Flink session: $APP"

if "$FLINK/flink" list -t yarn-session -Dyarn.application.id="$APP" 2>/dev/null | grep -q "ops-analytics (RUNNING)"; then
  echo "The ops-analytics job is already running"
  exit 0
fi

{
  echo "SET 'execution.target' = 'yarn-session';"
  echo "SET 'yarn.application.id' = '$APP';"
  "$WORK/assemble-job.sh" "$BOOTSTRAP" SSL
} > "$WORK/ops-analytics.sql"
"$FLINK/sql-client.sh" -f "$WORK/ops-analytics.sql"
echo "Submitted the ops-analytics job to $APP"
