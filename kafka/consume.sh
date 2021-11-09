source kafka.sh

$KAFKA/bin/kafka-console-consumer.sh \
  --topic order \
  --bootstrap-server localhost:9092