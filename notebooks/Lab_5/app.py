from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/echo", methods=["POST"])  # akceptuj tylko POST
def echo():
    data = request.get_json()           # odczytaj ciało JSON
    return jsonify({
        "otrzymalem": data,
        "liczba_pol": len(data),
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
