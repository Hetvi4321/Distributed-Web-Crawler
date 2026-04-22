from flask import Flask, request, jsonify
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# MongoDB connection
client = MongoClient("mongodb://10.201.13.21:27017/")
db = client["search_engine"]
collection = db["inverted_index"]


@app.route("/")
def home():
    return "Search API is running!"


@app.route("/search")
def search():
    query = request.args.get("q")

    if not query:
        return jsonify({"error": "No query provided"}), 400

    # Step 1: Process query
    words = query.lower().split()

    scores = {}

    # Step 2: Collect results for each word
    for word in words:
        result = collection.find_one({"word": word})

        if not result:
            continue

        for entry in result["urls"]:
            url = entry["url"]
            freq = entry["freq"]

            # Step 3: Combine scores
            if url not in scores:
                scores[url] = 0

            scores[url] += freq

    # Step 4: Sort results
    sorted_results = sorted(
        scores.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # Step 5: Limit results
    top_results = sorted_results[:10]

    # Step 6: Format output
    output = [
        {"url": url, "score": score}
        for url, score in top_results
    ]

    return jsonify(output)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=True)