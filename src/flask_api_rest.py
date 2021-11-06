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
        # Aca debe ir kafka
        return jsonify({"response":"Orden generada correctamente"})
    return "ok"



if __name__ == "__main__":
    app.run(debug=True)