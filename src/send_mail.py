import json,smtplib, ssl
from kafka import KafkaConsumer

fromadrr='tarea2.sistdist'
frompass='Tarea2_sistDist2021'
SUMMARY = 'summary'


#create topic: /usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic summary --partitions 1 --replication-factor 1 --config retention.ms=259200000


if __name__=="__main__":
    
    summary_consumer = KafkaConsumer(SUMMARY,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            enable_auto_commit=True,
            auto_commit_interval_ms = 100,
            group_id='summmary',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))

    context = ssl.create_default_context()
    mail_msg='''
            Timestap: {}
            Numero de orden: {}
            Numero de sopaipas: {}'''
    server= smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls(context=context)
    server.ehlo()
    server.login(fromadrr,frompass)

    for msg in summary_consumer:
        print('\n mensaje ',msg)
        value=msg.value
        try:
            
            server.sendmail(fromadrr,value['email_vendedor'],mail_msg.format(msg.timestamp,value['order_id'],value['numero_sopaipillas']))

        except:
            print('Algo fallo al enviar el correo a ', msg)
    
    server.close()