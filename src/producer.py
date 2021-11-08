from kafka import KafkaProducer
import json 

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])
producer.send('event', {'id': 123, 'email_vendedor': 'asdas@mail.com'})
producer.flush()