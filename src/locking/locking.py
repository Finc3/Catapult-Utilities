import logging
import os
from contextlib import contextmanager
from functools import wraps
from threading import Thread
from time import sleep, time
from typing import Union
from uuid import uuid4

import pymongo
from pymongo import MongoClient, ReturnDocument
from pymongo.database import Database

logger = logging.getLogger(__name__)


class LockFailure(Exception):
    """Can be raised when a lock could not be established"""


class MongoLocks:
    _DEFAULT_DB = "mongo_locks"
    _COL = "locks"
    _MAX_AGE = 10  # the maximum age (in seconds) a lock can be held (without being refreshed) before it is considered stale and will be released

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
        self._launch_pid = None  # Used to ensure that a fully initialized MongoLocks instance does not get forked

    @contextmanager
    def lock_context(self, key: str, *, raise_exceptions: bool = False) -> bool:
        """Context manager to acquire and release an application-wide lock on a resource.

        Args:
            key (str): The name of the resource to lock.
            raise_exceptions (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
        """
        locked = self._acquire(key, self._MAX_AGE)
        if not locked and raise_exceptions:
            raise LockFailure
        try:
            yield locked
        except Exception as e:
            raise e
        finally:
            if locked:
                self._release(key)

    def lock(self, key: str, *, raise_exceptions: bool = False):
        """Decorator to acquire and release an application-wide lock on a resource.
        Silently ignores execution if `raise_exceptions` is False and lock could not be acquired.

        Args:
            key (str): The name of the resource to lock.
            raise_exceptions (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
        """

        key_name = key.__name__ if callable(key) else key

        def outer(f):
            @wraps(f)
            def inner():
                with self.lock_context(key_name, raise_exceptions=raise_exceptions) as lock:
                    if lock:
                        f()

            return inner

        if callable(key):
            # assuming simplified usage, e.g. key = f.__name__
            return outer(key)
        else:
            return outer

    def _acquire(self, key: str, expire_in=int):
        if self._disabled:
            return True
        if not self._initialized:
            self._initialized = True
            self._initialize()
        self._pid_check()
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
        self._locks.discard(key)
        self._client.delete_one({"_id": key})

    def _initialize(self):
        t = Thread(target=self._heartbeat_worker, daemon=True)
        t.start()
        self._launch_pid = os.getpid()

    def _pid_check(self):
        if os.getpid() != self._launch_pid:
            logger.error("MongoLocks instance was forked after initialization. This is not allowed.")
            raise RuntimeError("MongoLocks instance was forked after initialization. This is not allowed.")

    def _heartbeat_worker(self):
        while True:
            sleep(self._MAX_AGE // 3)
            for lock_key in tuple(self._locks):
                try:
                    self._client.find_one_and_update({"_id": lock_key}, {"$set": {"expires_at": time() + self._MAX_AGE}})
                except Exception as e:
                    logger.exception(f"Error while updating lock {lock_key}: {e}")
