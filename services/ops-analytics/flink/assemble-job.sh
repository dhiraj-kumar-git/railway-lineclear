#!/bin/sh
# Prints the operations analytics job as one SQL script for the Flink SQL client:
# the settings, every table, then every INSERT inside one statement set, so all of
# them run as a single Flink job reading each Kafka topic once.
# Usage: assemble-job.sh <Kafka bootstrap servers> <PLAINTEXT or SSL>

set -eu
here=$(cd "$(dirname "$0")" && pwd)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <Kafka bootstrap servers> <PLAINTEXT or SSL>" >&2
  exit 2
fi

{
  cat "$here/settings.sql"
  for table in "$here"/tables/*.sql; do cat "$table"; done
  echo "EXECUTE STATEMENT SET"
  echo "BEGIN"
  for job in "$here"/jobs/*.sql; do cat "$job"; done
  echo "END;"
} | sed -e "s|@KAFKA_BOOTSTRAP_SERVERS@|$1|g" -e "s|@KAFKA_SECURITY_PROTOCOL@|$2|g"
