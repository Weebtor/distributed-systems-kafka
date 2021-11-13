from datetime import date
import json
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer

SUMMARY = 'summary'
ORDER = 'order'

app = Flask(__name__)



@app.route("/")
def hello_world():
    return "<h1> Hello World </h1>"

@app.route("/newOrder", methods=["POST"])
def new_order():
    if request.method == "POST":
        order = request.json
        # print(order)
        # expect:
        # {
        #     "order_id": 1231,
        #     "email_vendedor": "asdas@gmail",
        #     "email_comprador": "asdas1@gmail.com",
        #     "numero_sopaipillas": 12312
        # }

        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])
        producer.send(ORDER, order)
        producer.flush()
        return jsonify({"response":"Orden generada correctamente"})
    return "ok"



@app.route("/dailySummary", methods = ["POST"])
def summary():
    if request.method == "POST":
        summary_consumer = KafkaConsumer(ORDER,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            # enable_auto_commit=True,
            # auto_commit_interval_ms = 100,
            group_id='dailySummary',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))
        
        today =  date.today()
        resumen = {
            "ventas": {},
            "fecha": today.strftime("%d/%m/%Y"), 
            }
        
        # Revisa todos los mensajes sin consumir a partir del offset registrado
        for message in summary_consumer:
            # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
            #     message.offset, message.key,
            #     message.value))
            
            # Genera el resumen
            if message.value['email_vendedor'] not in resumen["ventas"].keys():
                resumen["ventas"][message.value['email_vendedor']] = {
                    "n_sopaipillas": int(message.value['numero_sopaipillas']),
                    "n_ordenes": 1
                    }
            else: # hace los calculos
                resumen["ventas"][message.value['email_vendedor']]["n_sopaipillas"] = int(resumen["ventas"][message.value['email_vendedor']]["n_sopaipillas"]) + int(message.value['numero_sopaipillas'])
                resumen["ventas"][message.value['email_vendedor']]["n_ordenes"] = int(resumen["ventas"][message.value['email_vendedor']]["n_ordenes"]) + 1
        print(resumen)

        summary_consumer.commit() # Registra el ultimo offset
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])
        producer.send(SUMMARY, resumen)
        producer.flush()        
        

        return jsonify({"response":"Reporte generado correctamente"})
        

if __name__ == "__main__":
    app.run(debug=True)