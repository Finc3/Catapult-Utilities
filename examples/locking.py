from time import sleep

from pymongo import MongoClient

from locking import MongoLocks

"""
Example usage of MongoLocks
"""

mc = MongoClient()
mongo_locks = MongoLocks(mc, "my_project")


@mongo_locks.lock("op1")
def op1():
    # "op1" is now an operation protected by a lock for the duration of the method execution
    print("Working...")
    sleep(20)
    print("...Done")


@mongo_locks.lock
def op1():
    # Alternate syntax that uses the function name as the lock key
    # "op1" is now an operation protected by a lock for the duration of the method execution
    print("Working...")
    sleep(20)
    print("...Done")


def op1():
    # Use `lock_context' to acquire a locked context within a method
    with mongo_locks.lock_context("op1"):
        # "op1" is now an operation protected by a lock for the duration of the method execution
        print("Working...")
        sleep(20)
        print("...Done")


op1()
