source kafka.sh

$KAFKA/bin/kafka-console-consumer.sh \
  --topic summary \
  --bootstrap-server localhost:9092