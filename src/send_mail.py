import json,smtplib, ssl
from kafka import KafkaConsumer
from datetime import date

fromadrr='tarea2.sistdist'
frompass='Tarea2_sistDist2021'
SUMMARY = 'summary'

if __name__=="__main__":
    # Consume del topico "summary"
    # Configurado para que no hacer break del loop 
    summary_consumer = KafkaConsumer(SUMMARY,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms = 100,
            group_id='summmary',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # Configuración para el envio del correo
    context = ssl.create_default_context()
    mail_msg='''
            Reporte de sopaipillas.

            Fecha:{}
            Cantidad de ordenes recibidas: {}
            Cantidad de sopaipillas vendidas: {}
            
            '''
    server= smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls(context=context)
    server.ehlo()
    server.login(fromadrr,frompass)

    # Inicia la aplicación y lee todos los mensajes desde el offset
    print("Esperando Kafka")
    for msg in summary_consumer:
        
        # resumen={'ventas': {'vendor_tarea2@yopmail.com': {'n_sopaipillas': 246246, 'n_ordenes': 2}, 'vendor2_tarea2@yopmail.com': {'n_sopaipillas': 246246, 'n_ordenes': 2}}, 'fecha': date.today().strftime("%d/%m/%Y") }
        # print(msg)
        for mail in msg.value['ventas'].keys():        
            try:
                # print(msg.value['fecha'],msg.value['ventas'][mail]['n_ordenes'],msg.value['ventas'][mail]['n_sopaipillas'])
                server.sendmail(fromadrr,mail,mail_msg.format(msg.value['fecha'],msg.value['ventas'][mail]['n_ordenes'],msg.value['ventas'][mail]['n_sopaipillas']))
                print(f"Reporte enviado correctamente a {mail}")
                summary_consumer.commit()
            except Exception as e:
                print(e)
                print('\nAlgo fallo al enviar el correo a ')

        print("Esperando Kafka")
            
    
    server.close()