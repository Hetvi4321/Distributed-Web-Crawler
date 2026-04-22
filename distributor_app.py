from flask import Flask, request, jsonify
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["crawler_db"]

queue_collection = db["queue"]
visited_collection = db["visited"]

# 🔥 Ensure uniqueness (VERY IMPORTANT)
queue_collection.create_index([("url", ASCENDING)], unique=True)
visited_collection.create_index([("url", ASCENDING)], unique=True)


@app.route("/")
def home():
    return "Distributor Service is running!"


# ✅ 1. Add URLs (NO duplicates)
@app.route("/add_url", methods=["POST"])
def add_url():
    data = request.get_json()

    if not data:
        return jsonify({"error": "No data provided"}), 400

    if "url" in data:
        urls = [data["url"]]
    elif "urls" in data:
        urls = data["urls"]
    else:
        return jsonify({"error": "No URL field found"}), 400

    added_urls = []

    for url in urls:
        if not isinstance(url, str) or not url.startswith("http"):
            continue

        try:
            # 🔥 Only insert if not visited
            if not visited_collection.find_one({"url": url}):
                queue_collection.insert_one({"url": url})
                added_urls.append(url)

        except DuplicateKeyError:
            # Already exists in queue
            continue

    return jsonify({
        "status": "success",
        "added_count": len(added_urls),
        "added_urls": added_urls
    })


# ✅ 2. Get URL (ATOMIC — NO duplication)
@app.route("/get_url", methods=["GET"])
def get_url():
    # 🔥 Atomic fetch + delete
    url_doc = queue_collection.find_one_and_delete({})

    if url_doc:
        url = url_doc["url"]

        try:
            visited_collection.insert_one({"url": url})
        except DuplicateKeyError:
            pass

        return jsonify({
            "status": "success",
            "url": url
        })

    return jsonify({
        "status": "empty",
        "url": None
    })


# ✅ 3. Status
@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "queue_length": queue_collection.count_documents({}),
        "visited_count": visited_collection.count_documents({})
    })


# ✅ 4. Health check
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/clear", methods=["POST"])
def clear():
    queue_collection.delete_many({})
    visited_collection.delete_many({})

    return jsonify({
        "status": "cleared"
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)