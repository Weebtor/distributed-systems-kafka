#!/bin/bash
source kafka.sh
$KAFKA/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092