from flask import Flask, request, jsonify
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["crawler_db"]

queue_collection = db["queue"]
visited_collection = db["visited"]
processing_collection = db["processing"]
processing_collection.create_index([("url", ASCENDING)], unique=True)

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
from datetime import datetime

@app.route("/get_url", methods=["GET"])
def get_url():
    url_doc = queue_collection.find_one_and_delete({})

    if url_doc:
        url = url_doc["url"]

        try:
            processing_collection.insert_one({
                "url": url,
                "timestamp": datetime.utcnow()
            })
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
        "queue": queue_collection.count_documents({}),
        "processing": processing_collection.count_documents({}),
        "visited": visited_collection.count_documents({})
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

@app.route("/done", methods=["POST"])
def done():
    data = request.get_json()
    url = data.get("url")

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    processing_collection.delete_one({"url": url})

    try:
        visited_collection.insert_one({"url": url})
    except DuplicateKeyError:
        pass

    return jsonify({"status": "completed"})

from datetime import timedelta

@app.route("/reset_stuck", methods=["POST"])
def reset_stuck():
    timeout = datetime.utcnow() - timedelta(minutes=2)

    stuck_jobs = processing_collection.find({
        "timestamp": {"$lt": timeout}
    })

    reset_count = 0

    for job in stuck_jobs:
        url = job["url"]

        queue_collection.insert_one({"url": url})
        processing_collection.delete_one({"url": url})
        reset_count += 1

    return jsonify({
        "status": "reset",
        "count": reset_count
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)