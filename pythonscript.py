from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
db=client.twg2nk
collection=db.dp2

direct = "/workspace/ds2002-dp2/data"
i = 0

for filename in os.listdir(direct):
    if filename.endswith('.json'):
        filepath = os.path.join(direct, filename)
        with open(filepath) as f:
            try:
                data = json.load(f)
                if len(data) == 1:
                    collection.insert_one(data)
                if len(data) > 1:
                    collection.insert_many(data)
            except:
                pass