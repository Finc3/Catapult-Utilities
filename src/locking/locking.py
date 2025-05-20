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


def _make_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    return logger


class LockFailure(Exception):
    """Can be raised when a lock could not be established"""


class MongoLocks:
    _DEFAULT_DB = "mongo_locks"
    _COL = "locks"
    _MAX_AGE = 10  # the maximum age (in seconds) a lock can be held (without being refreshed) before it is considered stale and will be released
    _POOLING_INTERVAL = 100  # the interval (in milliseconds) to try to acquire a lock

    def __init__(self, client: Union[MongoClient, Database], namespace: str, disabled: bool = False, logger=None):
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
        self.logger = logger if logger is not None else _make_logger()

    @contextmanager
    def lock_context(self, key: str, *, raise_exceptions: bool = False, wait_for: int = 0) -> bool:
        """Context manager to acquire and release an application-wide lock on a resource.

        Args:
            key (str): The name of the resource to lock.
            raise_exceptions (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
            wait_for (int, optional): The maximum time (in seconds) to wait for a lock to be acquired.
        """
        locked = self._acquire(key, self._MAX_AGE, wait_for)
        if not locked and raise_exceptions:
            raise LockFailure
        try:
            yield locked
        except Exception as e:
            raise e
        finally:
            if locked:
                self._release(key)

    def lock(self, key: str, *, raise_exceptions: bool = False, wait_for: int = 0):
        """Decorator to acquire and release an application-wide lock on a resource.
        Silently ignores execution if `raise_exceptions` is False and lock could not be acquired.

        Args:
            key (str): The name of the resource to lock.
            raise_exceptions (bool, optional): Whether or not to raise `LockFailure` when a lock could not be achieved.
            wait_for (int, optional): The maximum time (in seconds) to wait for a lock to be acquired.
        """

        def decorator(f):
            key_name = key or f.__name__

            @wraps(f)
            def wrapped(*args, **kwargs):
                with self.lock_context(key_name, raise_exceptions=raise_exceptions, wait_for=wait_for) as lock:
                    if lock:
                        return f(*args, **kwargs)

            return wrapped

        if callable(key):
            func = key
            key = func.__name__
            return decorator(func)
        return decorator

    def _acquire(self, key: str, expire_in: int = 0, wait_for: int = 0) -> bool:
        if self._disabled:
            return True
        if not self._initialized:
            self._initialized = True
            self._initialize()
        self._pid_check()
        key = f"{self._ns}__{key}"
        expire_at = time() + expire_in
        id_ = str(uuid4())

        lock = self._try_acquire(key, expire_at, id_)
        if lock is None and wait_for:
            _start = time()
            while lock is None:
                if time() - _start > wait_for:
                    break
                lock = self._try_acquire(key, expire_at, id_)
                sleep(self._POOLING_INTERVAL / 1000)

        locked = lock is not None and (lock["lock_id"] == id_)
        if locked:
            self.logger.debug(f"Sucessfully acquired lock for {key}")
            self._locks.add(key)
        else:
            self.logger.debug(f"Failed to acquire lock for {key}")
        return locked

    def _try_acquire(self, key: str, expire_at: int, id_: str):
        try:
            return self._client.find_one_and_replace(
                {"_id": key, "$or": [{"expires_at": {"$exists": False}}, {"expires_at": {"$lte": time()}}]},
                {"expires_at": expire_at, "lock_id": id_},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except pymongo.errors.DuplicateKeyError:
            return None

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
        self.logger.debug(f"Heartbeat thread initialized, registered launch pid: {self._launch_pid}")

    def _pid_check(self):
        if self._launch_pid not in (None, os.getpid()):
            self.logger.error("MongoLocks instance was forked after initialization. This is not allowed.")
            raise RuntimeError("MongoLocks instance was forked after initialization. This is not allowed.")

    def _heartbeat_worker(self):
        while True:
            sleep(self._MAX_AGE // 3)
            for n in range(3):
                try:
                    self._client.update_many({"_id": {"$in": list(self._locks)}}, {"$set": {"expires_at": time() + self._MAX_AGE}})
                    break
                except Exception as e:
                    err = e
                    sleep(1 * n)
            else:
                self.logger.exception(f"Error while updating locks: {err}")
