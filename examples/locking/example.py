from time import sleep

from pymongo import MongoClient

from locking import MongoLocks

"""
Example usage of MongoLocks
"""

mc = MongoClient()
mongo_locks = MongoLocks(mc, "my_project")


# Second decorator to test decorator chaining
def say_hello(f):
    def wrapper(*args, **kwargs):
        print("Hello from the second decorator!")
        return f(*args, **kwargs)

    return wrapper


@mongo_locks.lock
@say_hello
def op1():
    # Alternate syntax that uses the function name as the lock key
    # "op1" is now an operation protected by a lock for the duration of the method execution
    print("Callable")
    print("Working...")
    sleep(20)
    print("...Done")


@mongo_locks.lock("op1")
@say_hello
def _op1():
    # "op1" is now an operation protected by a lock for the duration of the method execution
    print("Key!")
    print("Working...")
    sleep(20)
    print("...Done")


def _op1_():
    # Use `lock_context' to acquire a locked context within a method
    with mongo_locks.lock_context("op1") as lock:
        if lock:
            # If `lock` is True, "op1" is now an operation protected by a lock for the duration of the method execution
            print("Context")
            print("Working...")
            sleep(20)
            print("...Done")


_op1()
