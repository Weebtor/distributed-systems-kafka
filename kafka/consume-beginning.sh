#!/bin/bash

source kafka.sh

$KAFKA/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic order \
    --from-beginning \
    --max-messages 100 \