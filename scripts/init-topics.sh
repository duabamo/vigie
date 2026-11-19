#!/usr/bin/env bash
set -euo pipefail
for topic in financial-transactions fraud-alerts invalid-transactions; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done

/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:19092 \
  --entity-type topics --entity-name financial-transactions \
  --alter --add-config message.timestamp.type=LogAppendTime
