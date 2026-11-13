#!/bin/bash
# EMR bootstrap action, run on every node before Flink starts: installs Fluent Bit and sends
# the Flink JobManager and TaskManager logs, and the step logs, to Loki through the internal
# load balancer. If Fluent Bit cannot be installed, the cluster still starts: the logs then
# stay on the nodes and in the EMR log folder in S3.
# Usage: ship-logs-to-loki.sh <Loki address> <port>

set -uo pipefail
LOKI_HOST="$1"
LOKI_PORT="$2"

NODE=$(hostname -s)
sudo tee /etc/yum.repos.d/fluent-bit.repo > /dev/null <<'REPO'
[fluent-bit]
name=Fluent Bit
baseurl=https://packages.fluentbit.io/amazonlinux/2023/
gpgcheck=1
gpgkey=https://packages.fluentbit.io/fluentbit.key
enabled=1
REPO
installed=no

for _ in 1 2 3; do
  if sudo dnf install -y -q fluent-bit; then
    installed=yes
    break
  fi
  sleep 10
done

if [ "$installed" != yes ]; then
  echo "Fluent Bit could not be installed; Flink logs stay on the nodes and in S3"
  exit 0
fi
sudo mkdir -p /var/lib/fluent-bit

# YARN keeps each container's logs under /var/log/hadoop-yarn/containers; steps log under /mnt/var/log/hadoop/steps
sudo tee /etc/fluent-bit/fluent-bit.conf > /dev/null <<CONF
[SERVICE]
    Flush        5
    Log_Level    info
[INPUT]
    Name              tail
    Path              /var/log/hadoop-yarn/containers/*/*/*.log,/mnt/var/log/hadoop/steps/*/stdout,/mnt/var/log/hadoop/steps/*/stderr
    Tag               flink
    Refresh_Interval  10
    Read_from_Head    true
    DB                /var/lib/fluent-bit/flink-logs.db
    Mem_Buf_Limit     16MB
    Skip_Long_Lines   On
[OUTPUT]
    Name             loki
    Match            *
    Host             ${LOKI_HOST}
    Port             ${LOKI_PORT}
    Labels           job=flink, cluster=lineclear-analytics, node=${NODE}
    Line_Format      key_value
    Drop_Single_Key  raw
    Retry_Limit      no_limits
CONF
sudo systemctl enable --now fluent-bit
echo "Fluent Bit sends Flink logs from ${NODE} to ${LOKI_HOST}:${LOKI_PORT}"
