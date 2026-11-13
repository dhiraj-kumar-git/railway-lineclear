#!/bin/bash
# Stops one Flink task manager by force, as a crashed worker would stop. Runs as an EMR
# step on the primary node. YARN starts a new task manager and Flink resumes the job from
# its last completed checkpoint in S3; the JobManager log then says "Restoring job".

set -euo pipefail
SESSION_NAME=lineclear-analytics

APP=$(yarn application -list -appStates RUNNING 2>/dev/null | awk -v name="$SESSION_NAME" '$2 == name {print $1}' | head -n 1)

if [ -z "$APP" ]; then
  echo "No running Flink session named $SESSION_NAME" >&2
  exit 1
fi

ATTEMPT=$(yarn applicationattempt -list "$APP" 2>/dev/null | awk '$1 ~ /^appattempt_/ {print $1}' | tail -n 1)

# Container 000001 is the JobManager; every other container is a task manager
CONTAINER=$(yarn container -list "$ATTEMPT" 2>/dev/null | awk '$1 ~ /^container_/ {print $1}' | grep -v '_000001$' | head -n 1)

if [ -z "$CONTAINER" ]; then
  echo "No task manager container found in $ATTEMPT" >&2
  exit 1
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Stopping task manager $CONTAINER of $APP"
yarn container -signal "$CONTAINER" FORCEFUL_SHUTDOWN
