from pymongo import MongoClient

client = MongoClient("mongodb://10.201.13.21:27017/")
db = client["search_engine"]

# Inverted index collection
index_collection = db["inverted_index"]

# 🔥 Index for faster search
index_collection.create_index("word")
