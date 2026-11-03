"""Tests for CrawlLock against an in-memory stand-in for Firestore transactions."""

from datetime import datetime, timedelta, timezone

import pytest

from cache_layer import crawl_lock
from cache_layer.crawl_lock import CrawlLock


class _Snap:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _Ref:
    def __init__(self, store, key):
        self.store, self.key = store, key

    def get(self, transaction=None):
        return _Snap(self.store.get(self.key))


class _Txn:
    def set(self, ref, data):
        ref.store[ref.key] = dict(data)

    def delete(self, ref):
        ref.store.pop(ref.key, None)


class _DB:
    def __init__(self):
        self.store = {}

    def collection(self, name):
        db = self

        class _Col:
            def document(self, doc_id):
                return _Ref(db.store, (name, doc_id))
        return _Col()

    def transaction(self):
        return _Txn()


@pytest.fixture
def lock(monkeypatch):
    # The real decorator retries on contention; here each call runs once.
    monkeypatch.setattr(crawl_lock.firestore, "transactional", lambda fn: fn, raising=False)
    return CrawlLock(_DB(), "v3_gs_crawl_locks", ttl_minutes=10)


def test_first_acquire_wins_second_is_refused(lock):
    t1 = lock.acquire("A")
    assert t1
    assert lock.acquire("A") is None
    assert lock.acquire("B")  # other authors are independent


def test_release_frees_the_lock(lock):
    t1 = lock.acquire("A")
    assert lock.release("A", t1) is True
    assert lock.acquire("A")


def test_stale_lock_is_taken_over_and_old_owner_cannot_release_it(lock):
    t_old = lock.acquire("A")
    key = ("v3_gs_crawl_locks", "A")
    lock.db.store[key]["started_at"] = datetime.now(timezone.utc) - timedelta(minutes=30)
    t_new = lock.acquire("A")
    assert t_new and t_new != t_old
    assert lock.release("A", t_old) is False  # the expired owner must not delete the new lock
    assert lock.db.store[key]["token"] == t_new
    assert lock.acquire("A") is None


def test_release_of_missing_lock_is_harmless(lock):
    assert lock.release("A", "nope") is False
