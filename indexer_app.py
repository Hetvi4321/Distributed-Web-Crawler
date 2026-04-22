from flask import Flask, request, jsonify
from utils import extract_text, process_text, count_words, extract_links
from db import index_collection
from distributor_client import send_links

app = Flask(__name__)

@app.route("/index", methods=["POST"])
def index():
    try:
        data = request.json
        url = data.get("url")
        html = data.get("html")

        if not url or not html:
            return jsonify({"error": "Missing url or html"}), 400

        print(f"\n[Indexing] {url}")

        # ✅ Step 1: Extract text
        text = extract_text(html)

        # ✅ Step 2: Process words
        words = process_text(text)

        # ✅ Step 3: Count frequency
        freq = count_words(words)

        # ✅ Step 4: Extract clean links
        links = extract_links(html, url)
        send_links(links)

        # ✅ Step 5: Store inverted index (NO DUPLICATES)
        for word, count in freq.items():

            # Update if URL already exists
            index_collection.update_one(
                {"word": word, "urls.url": url},
                {"$set": {"urls.$.freq": count}}
            )

            # Insert if new URL
            index_collection.update_one(
                {"word": word, "urls.url": {"$ne": url}},
                {
                    "$push": {
                        "urls": {
                            "$each": [{
                                "url": url,
                                "freq": count
                            }],
                            "$slice": -50  # limit size
                        }
                    }
                },
                upsert=True
            )

        print(f"Words: {len(freq)} | Links: {len(links)}")

        return jsonify({
            "status": "indexed",
            "url": url,
            "words_processed": len(freq),
            "links_found": len(links)
        })

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
