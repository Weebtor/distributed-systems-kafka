#!/bin/bash

source kafka.sh

$KAFKA/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --config retention.ms=259200000 \
    --topic order

$KAFKA/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --config retention.ms=259200000 \
    --topic summary
