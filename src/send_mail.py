import json,smtplib, ssl
from kafka import KafkaConsumer
from datetime import date

fromadrr='tarea2.sistdist'
frompass='Tarea2_sistDist2021'
SUMMARY = 'summary'


#create topic: /usr/local/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic summary --partitions 1 --replication-factor 1 --config retention.ms=259200000


if __name__=="__main__":
    
    summary_consumer = KafkaConsumer(SUMMARY,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms = 100,
            group_id='summmary',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))

    context = ssl.create_default_context()
    mail_msg='''
            Fecha:{}
            Cantidad de ordenes recibidas: {}
            Cantidad de sopaipillas vendidas: {}'''
    server= smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls(context=context)
    server.ehlo()
    server.login(fromadrr,frompass)
    
    print("Esperando Kafka")
    for msg in summary_consumer:
        # resumen={'ventas': {'vendor_tarea2@yopmail.com': {'n_sopaipillas': 246246, 'n_ordenes': 2}, 'vendor2_tarea2@yopmail.com': {'n_sopaipillas': 246246, 'n_ordenes': 2}}, 'fecha': date.today().strftime("%d/%m/%Y") }
        print(msg)
        for mail in msg.value['ventas'].keys():        
            try:               
                server.sendmail(fromadrr,mail,mail_msg.format(msg.value['fecha'],msg.value['ventas'][mail]['n_ordenes'],msg.value['ventas'][mail]['n_sopaipillas']))
                print(f"Se envio correctamente el correo a {mail}")
            except:
                print('\nAlgo fallo al enviar el correo a ', msg)
            
    
    server.close()