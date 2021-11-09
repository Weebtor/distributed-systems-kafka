#!/bin/bash

source kafka.sh

$KAFKA/bin/kafka-console-producer.sh \
  --topic summarie \
  --bootstrap-server localhost:9092