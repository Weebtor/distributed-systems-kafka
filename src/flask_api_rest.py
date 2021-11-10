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

        # Aca debe ir kafka y generar topic
        return jsonify({"response":"Orden generada correctamente"})
    return "ok"

@app.route("/dailySummary", methods = ["POST"])
def daily_summary():
    if request.method == "POST":
        to = request.json
        print(to)
        summary_consumer = KafkaConsumer(ORDER,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            enable_auto_commit=True,
            auto_commit_interval_ms = 100,
            group_id='daily',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))
        for message in summary_consumer:
            #print(message)
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))
        # 
        # {
        #     "order_id": 1231,
        #     "email_vendedor": "asdas@gmail",
        #     "email_comprador": "asdas1@gmail.com",
        #     "numero_sopaipillas": 12312
        # }
        
        
        
        summary_consumer.commit()
        return jsonify({"response":"Reporte generado correctamente"})

@app.route("/summary", methods = ["POST"])
def summary():
    if request.method == "POST":
        to = request.json
        print(to)
        summary_consumer = KafkaConsumer(ORDER,
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            enable_auto_commit=True,
            auto_commit_interval_ms = 100,
            group_id='daily',
            value_deserializer=lambda m: json.loads(m.decode('ascii')))
        ordenes = {}
        cocinero = {}
        for message in summary_consumer:
            #print(message)
            n_sopai = (message.value['numero_sopaipillas'])
            id_vendedor = (message.value['email_vendedor'])
            id_cocinero = (message.value['email_comprador'])
            #print (id_email, n_sopai)
            
            for key in ordenes.key():
                if (key == id_vendedor):
                    ordenes[key] += n_sopai
                    break
            else:
                ordenes.setdefault(id_cocinero, n_sopai)
            print(ordenes)  
            for key in cocinero.keys():
                if(key == id_cocinero):
                    cocinero[key]  += n_sopai
                    break
            else:
                cocinero.setdefault(id_cocinero, n_sopai)
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])
        producer.send(SUMMARY, ordenes)
        producer.send(SUMMARY, cocinero)
        producer.flush()        
        summary_consumer.commit()

        return jsonify({"response":"Reporte generado correctamente"})

def enviar_correo():
    pass
        

if __name__ == "__main__":
    app.run(debug=True)