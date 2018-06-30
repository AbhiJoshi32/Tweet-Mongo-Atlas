from pymongo import MongoClient
from config import ATLAST_CONNECTION


class DBHelper:
    def __init__(self, db_name):
        self.mongo_client = MongoClient(ATLAST_CONNECTION)
        self.db_name = db_name
        self.db = self.mongo_client[db_name]

    def insert_doc(self, doc, collection):
        return self.db[collection].insert_one(doc)

    def find_docs(self, query, collection):
        return self.db[collection].find(query)

    def find_one_doc(self, query, collection):
        return self.db[collection].find_one(query)
