#!/bin/bash

source kafka.sh

$KAFKA/bin/kafka-console-producer.sh \
  --topic order \
  --bootstrap-server localhost:9092