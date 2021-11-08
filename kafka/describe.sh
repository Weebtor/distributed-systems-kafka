#!/bin/bash
source kafka.sh
$KAFKA/bin/kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic summary