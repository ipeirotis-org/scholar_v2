"""Per-author lock for on-demand GS crawls, held in Firestore.

The loading page re-enqueues a populate task on every reload, so without a
lock several tasks would crawl (and pay SerpApi for) the same new author.

Acquire and release both run in Firestore transactions: two tasks that find
the same abandoned lock cannot both take it over (the losing transaction is
retried and sees the new owner), and each acquisition carries a random token
so a task only ever deletes the lock it owns.
"""

import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import firestore


def _take(transaction, ref, token, now, ttl):
    snap = ref.get(transaction=transaction)
    if snap.exists:
        started = (snap.to_dict() or {}).get("started_at")
        if started and now - started < ttl:
            return False  # held by a live crawl
    transaction.set(ref, {"started_at": now, "token": token})
    return True


def _drop(transaction, ref, token):
    snap = ref.get(transaction=transaction)
    if snap.exists and (snap.to_dict() or {}).get("token") == token:
        transaction.delete(ref)
        return True
    return False  # expired and taken over by another task: not ours to delete


class CrawlLock:
    def __init__(self, db, collection, ttl_minutes):
        self.db = db
        self.collection = collection
        self.ttl = timedelta(minutes=ttl_minutes)

    def _ref(self, scholar_id):
        return self.db.collection(self.collection).document(scholar_id)

    def acquire(self, scholar_id):
        """Return an ownership token, or None if another live crawl holds the lock.

        Firestore errors propagate; callers decide whether to proceed unlocked.
        """
        token = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        take = firestore.transactional(_take)
        if take(self.db.transaction(), self._ref(scholar_id), token, now, self.ttl):
            return token
        return None

    def release(self, scholar_id, token):
        """Delete the lock only if `token` still owns it. Returns True if deleted."""
        drop = firestore.transactional(_drop)
        return drop(self.db.transaction(), self._ref(scholar_id), token)
