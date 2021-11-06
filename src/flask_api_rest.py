import re
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<h1> Hello World </h1>"

@app.route("/newOrder", methods=["POST"])
def new_order():
    if request.method == "POST":
        order = request.json
        print(order)
        # expect:
        # {
        #     "order_id": 1231,
        #     "email_vendedor": "asdas@gmail",
        #     "email_comprador": "asdas1@gmail.com",
        #     "numero_sopaipillas": 12312
        # }

        # Aca debe ir kafka y generar topic
        return jsonify({"response":"Orden generada correctamente"})
    return "ok"

@app.route("/dailySummary", methods = ["POST"])
def daily_summary():
    if request.method == "POST":
        
        # {
        #     "order_id": 1231,
        #     "email_vendedor": "asdas@gmail",
        #     "email_comprador": "asdas1@gmail.com",
        #     "numero_sopaipillas": 12312
        # }
        return jsonify({"response":"Reporte generado correctamente"})



def enviar_correo():
    pass
        

if __name__ == "__main__":
    app.run(debug=True)