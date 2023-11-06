from time import sleep

from pymongo import MongoClient

from locking import MongoLocks

"""
Example usage of MongoLocks
"""

mc = MongoClient()
mongo_locks = MongoLocks(mc, "my_project")


@mongo_locks.with_lock("op1")
def op1():
    # This operation is now protected by a lock for the duration of the method execution
    print("Working...")
    sleep(20)
    print("...Done")


op1()
