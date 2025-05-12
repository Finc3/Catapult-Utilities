from datetime import datetime
from pprint import pprint
from time import sleep

from pymongo import MongoClient

from locking import MongoLocks

"""
Script to monitor the locks collection in mongo
"""

mc = MongoClient()
mongo_locks = MongoLocks(mc, "example_project")
while True:
    locks = mc["mongo_locks"]["locks"].find()
    now = datetime.now().timestamp()
    expirations = [(n["_id"], (n["expires_at"] - now)) for n in locks]
    pprint(expirations)
    sleep(1)
