
from pymongo import MongoClient


def mongo_connection():
    return MongoClient("mongodb://mongo:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.0")
