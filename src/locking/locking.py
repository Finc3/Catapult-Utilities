from contextlib import contextmanager
from functools import wraps
from threading import Thread
from time import sleep, time
from typing import Union
from uuid import uuid4

import pymongo
from pymongo import MongoClient, ReturnDocument
from pymongo.database import Database


class LockFailure(Exception):
    """Can be raised when a lock could not be established"""


class MongoLocks:
    _DEFAULT_DB = "mongo_locks"
    _COL = "locks"

    def __init__(self, client: Union[MongoClient, Database], namespace: str, disabled: bool = False):
        """
        _summary_

        Args:
            client: Pass either a MongoClient or MongoClient.Database object
            namespace: A string that will be prefixed to all lock keys acquired by this instance
            disabled: When set to True, all lock attempts will be ignored. Intended for use during development.

        """
        self._disabled = disabled
        if disabled:
            return

        self._ns = namespace
        if isinstance(client, MongoClient):
            self._client = client[self._DEFAULT_DB][self._COL]
        elif isinstance(client, Database):
            self._client = client[self._COL]
        else:
            raise TypeError(f"Invalid {client=}, must be mongoclient or database")
        self._con_id = (self._client.database.client.HOST, self._client.database.client.PORT)
        self._locks = set()
        self._initialized = False

    @contextmanager
    def lock(self, key: str, expire_in: int = 60, raise_on_failure: bool = True) -> bool:
        """Context manager to acquire and release an application-wide lock on a resource.

        Args:
            key (str): The name of the resource to lock.
            expire_in (int, optional): For how long the lock should be held in seconds. Should be longer than the expected duration of the operation. Defaults to 600.
            raise_on_failure (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
        """
        locked = self._acquire(key, expire_in)
        if not locked and raise_on_failure:
            raise LockFailure
        yield locked
        if locked:
            self._release(key)

    def with_lock(self, key: str, expire_in: int = 60, raise_on_failure: bool = False):
        """Decorator to acquire and release an application-wide lock on a resource.
        Silently ignores execution if `raise_on_failure` is False and lock could not be acquired.

        Args:
            key (str): The name of the resource to lock.
            expire_in (int, optional): For how long the lock should be held in seconds. Should be longer than the expected duration of the operation. Defaults to 600.
            raise_on_failure (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
        """

        def outer(f):
            @wraps(f)
            def inner():
                with self.lock(key, expire_in=expire_in, raise_on_failure=raise_on_failure) as lock:
                    if lock:
                        f()

            return inner

        return outer

    def _acquire(self, key: str, expire_in=int):
        if self._disabled:
            return True
        if not self._initialized:
            self._initialize()
        key = f"{self._ns}__{key}"
        expire_at = time() + expire_in
        id_ = str(uuid4())
        try:
            res = self._client.find_one_and_replace(
                {"_id": key, "$or": [{"expires_at": {"$exists": False}}, {"expires_at": {"$lte": time()}}]},
                {"expires_at": expire_at, "lock_id": id_},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except pymongo.errors.DuplicateKeyError:
            return False
        locked = res["lock_id"] == id_
        if locked:
            self._locks.add(key)
        return locked

    def _release(self, key: str):
        if self._disabled:
            return
        key = f"{self._ns}__{key}"
        self._client.delete_one({"_id": key})
        self._locks.discard(key)

    def _initialize(self):
        t = Thread(target=self._heartbeat_worker, daemon=True)
        t.start()

    def _heartbeat_worker(self):
        while True:
            sleep(10)
            for lock_key in self._locks:
                self._client.find_one_and_update({"_id": lock_key}, {"$inc": {"expires_at": 10}})
