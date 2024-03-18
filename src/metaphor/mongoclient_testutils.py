
from pymongo import MongoClient


def mongo_connection():
    return MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.0")
