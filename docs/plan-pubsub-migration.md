# Plan: Cloud Tasks → Pub/Sub Migration

> Goal: Make the crawler pipeline robust against silent failures. Every task that fails should be tracked, visible, and recoverable.

---

## Executive Summary

The core problem isn't Cloud Tasks vs Pub/Sub — it's **silent failure on standard queues** and **no persistent failure tracking**. This plan presents two options:

1. **Option A (Recommended): Incremental fix** — Add dead-letter to all Cloud Tasks queues + build a failure tracking system in Firestore/BigQuery. Solves the problem with minimal disruption.
2. **Option B: Full Pub/Sub migration** — Replace Cloud Tasks with Pub/Sub topics/subscriptions. Gives native dead-letter everywhere + better observability, but requires significant rework of rate limiting and dedup.

---

## Current Pain Points

| Problem | Impact | Root Cause |
|---------|--------|------------|
| Standard queues have no dead-letter | Tasks silently vanish after 10 retries | `process-authors` and `process-pubs` queues lack `deadLetterConfig` |
| No failure tracking | Can't tell which authors/pubs failed | Dead-letter handler only logs to Cloud Logging — no queryable record |
| `refresh_error_authors()` is the only recovery | Runs daily at 03:00 UTC with 24h cooldown; misses inter-cycle failures | No event-driven recovery path |
| Partial publication enqueue failure is invisible | Author task returns 200 even when some pub enqueues fail | `enqueue_publications()` tolerates partial failure by design |
| No visibility into queue state | Can't see what's pending, what's retrying, what's stuck | Cloud Tasks has limited observability APIs |

---

## Option A: Fix Silent Failures (Keep Cloud Tasks)

**Effort: ~2–3 days. Zero architectural risk.**

### A1. Add dead-letter to ALL queues

Currently only `process-authors-priority` and `process-pub-priority` have dead-letter configured. Add it to the remaining 4 queues:

```
process-authors      → crawler-task-deadletter (Pub/Sub topic)
process-pubs         → crawler-task-deadletter (Pub/Sub topic)
cache-priority       → cache-task-deadletter (new Pub/Sub topic)
cache-batch          → cache-task-deadletter (new Pub/Sub topic)
```

**Changes:**
- `deploy-infrastructure.yml`: Add `deadLetterConfig` REST API PATCH for all 4 queues
- Create `cache-task-deadletter` Pub/Sub topic + push subscription → dead-letter handler
- Update `dead_letter_handler.py` to also handle cache task types (`populate_author_profile`, `invalidate_author`, `warm_author`)

### A2. Build persistent failure tracking

Replace "log and forget" with a queryable failure record:

**Firestore collection: `task_failures`**
```json
{
  "document_id": "{task_type}_{identifier}",
  "task_type": "fetch_author",
  "identifier": "abc123",
  "scholar_id": "abc123",
  "author_pub_id": "",
  "priority": false,
  "failure_count": 3,
  "first_failure": "2026-03-20T10:00:00Z",
  "last_failure": "2026-03-26T03:00:00Z",
  "last_error_source": "process-authors",
  "status": "failed",        // "failed" | "retrying" | "resolved"
  "resolved_at": null
}
```

**Changes:**
- `dead_letter_handler.py`: Write to Firestore `task_failures` collection (upsert, increment count)
- New `frontend/routes.py` endpoint: `/admin/failures` — dashboard showing failed tasks
- `refresh_service.py`: When re-enqueuing an error author, mark the failure as `retrying`
- After successful crawl in `fetch_author.py`: Mark failure as `resolved` (if exists)

### A3. Track partial publication enqueue failures

**Changes to `crawler/task_enqueuer.py`:**
- `enqueue_publications()`: Collect failed `author_pub_id`s
- Write a summary to Firestore `task_failures` with `task_type: "enqueue_publications"` listing which pubs failed
- Or: If any pub enqueue fails, write to GCS as a "retry manifest" that a scheduled job can pick up

### A4. Add alerting

- Cloud Monitoring alert policy: trigger on `task_dead_lettered` log entries (already structured JSON)
- Optional: Pub/Sub message count metric on `crawler-task-deadletter` topic → alert if > 0 in 1 hour

### Pros of Option A
- Minimal code changes (~4 files modified, 1 new Firestore collection)
- No migration risk — existing queue behavior unchanged
- Rate limiting, dedup, priority all continue to work as-is
- Can be done in a single PR
- Zero cost increase (Cloud Tasks is free up to 1M tasks/month; Pub/Sub dead-letter volume is negligible)

### Cons of Option A
- Still tied to Cloud Tasks limitations (limited observability, no message filtering, no replay)
- Dedup via task names is still limited by the 1-hour tombstone window
- No native support for message ordering or exactly-once processing

---

## Option B: Full Pub/Sub Migration

**Effort: ~2–3 weeks. Moderate architectural risk.**

### Architecture

Replace 6 Cloud Tasks queues with Pub/Sub topics + push subscriptions:

| Current Queue | Pub/Sub Topic | Subscription | Dead-Letter Topic |
|---|---|---|---|
| `process-authors` | `crawler-authors` | `crawler-authors-push` (push → Cloud Function) | `crawler-authors-dlq` |
| `process-authors-priority` | `crawler-authors-priority` | `crawler-authors-priority-push` | `crawler-authors-priority-dlq` |
| `process-pubs` | `crawler-pubs` | `crawler-pubs-push` | `crawler-pubs-dlq` |
| `process-pub-priority` | `crawler-pubs-priority` | `crawler-pubs-priority-push` | `crawler-pubs-priority-dlq` |
| `cache-priority` | `cache-priority` | `cache-priority-push` | `cache-priority-dlq` |
| `cache-batch` | `cache-batch` | `cache-batch-push` | `cache-batch-dlq` |

Plus a shared `task-failures` topic for all DLQ handlers to publish to → Firestore writer.

### B1. Pub/Sub Publisher (replaces task enqueuers)

Each enqueuer file gets rewritten to publish messages instead of creating tasks:

```python
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

def publish_author_crawl(scholar_id, priority=False):
    topic = TOPIC_AUTHORS_PRIORITY if priority else TOPIC_AUTHORS
    data = json.dumps({"scholar_id": scholar_id}).encode()
    future = publisher.publish(topic, data, scholar_id=scholar_id)
    return future.result()
```

**Key difference:** No task names → no built-in dedup. Must handle differently (see B3).

### B2. Pub/Sub Subscriptions (replaces Cloud Tasks dispatch)

Push subscriptions deliver to the same Cloud Function/Run endpoints:

```yaml
# Push subscription config
ackDeadlineSeconds: 600        # 10 min for author fetches
deadLetterPolicy:
  deadLetterTopic: projects/scholar-version2/topics/crawler-authors-dlq
  maxDeliveryAttempts: 10
retryPolicy:
  minimumBackoff: 30s
  maximumBackoff: 300s
```

**Authentication:** Push subscriptions support OIDC tokens natively — same as Cloud Tasks.

### B3. Dedup Strategy (critical change)

Cloud Tasks dedup is based on task names. Pub/Sub has `enableMessageOrdering` + message dedup, but it's limited:
- Pub/Sub dedup window is only **10 minutes** (same as our current time-bucket strategy, so actually a good fit)
- Dedup is based on `messageId` which we can't set — or we can use `orderingKey` + application-level dedup

**Recommended approach: Application-level dedup via Firestore**

```python
# In the consumer (fetch_author):
def v3_fetch_author(request):
    scholar_id = payload["scholar_id"]
    dedup_ref = db.collection("task_dedup").document(f"author_{scholar_id}")
    dedup_doc = dedup_ref.get()

    if dedup_doc.exists:
        last_run = dedup_doc.get("timestamp")
        if (now - last_run) < timedelta(minutes=10):
            return {"status": "deduplicated"}, 200

    # Process the task...
    dedup_ref.set({"timestamp": now, "status": "processing"})
```

This is more explicit than Cloud Tasks tombstones and gives visibility into what's been processed.

### B4. Rate Limiting (critical change)

**Cloud Tasks has built-in rate limiting** (max dispatches/sec, max concurrent). Pub/Sub push subscriptions do NOT have this.

**Options:**
1. **Cloud Run concurrency limits** — Set `--concurrency=5` on the crawler Cloud Run service. This limits parallel processing but doesn't limit the push rate (messages queue up in the subscription).
2. **Pull subscriptions + Cloud Run Jobs** — Use pull subscriptions with a worker that controls its own concurrency. More complex but gives precise rate control.
3. **Flow control on subscribers** — Pub/Sub client libraries have `FlowControl(max_messages=5)` for pull subscriptions.
4. **Cloud Tasks as a rate limiter in front of Pub/Sub** — Hybrid: Pub/Sub for the message bus, Cloud Tasks for rate-limited dispatch. Defeats the purpose.

**Recommended:** Use **pull subscriptions** for crawler topics (where rate limiting matters for Scholar API) and **push subscriptions** for cache topics (where rate doesn't matter much).

```python
# Pull subscriber for crawler (runs as Cloud Run service)
subscriber = pubsub_v1.SubscriberClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=5)

def callback(message):
    process_author(json.loads(message.data))
    message.ack()

subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
```

This requires converting the crawler from a Cloud Function to a **long-running Cloud Run service** with a pull subscriber.

### B5. Priority Handling

Two approaches:
1. **Separate topics** (recommended, matching current architecture) — `crawler-authors` + `crawler-authors-priority` with different subscription configs (priority gets higher concurrency/faster ack)
2. **Single topic with message attributes** — Filter subscriptions by `priority=true`. Pub/Sub supports subscription-level filters.

### B6. Dead-Letter with Failure Tracking (native)

Every subscription gets a dead-letter topic. DLQ subscribers write to Firestore:

```python
def handle_dead_letter(message):
    payload = json.loads(message.data)
    failure_ref = db.collection("task_failures").document(f"{task_type}_{identifier}")
    failure_ref.set({
        "task_type": task_type,
        "identifier": identifier,
        "failure_count": firestore.Increment(1),
        "last_failure": datetime.utcnow(),
        "delivery_attempts": message.delivery_attempt,
        "status": "failed"
    }, merge=True)
    message.ack()
```

### B7. Migration Strategy (incremental, queue-by-queue)

**Phase 1: Cache queues** (lowest risk, simplest consumers)
1. Create `cache-priority` and `cache-batch` Pub/Sub topics + push subscriptions
2. Update `frontend/queue_client.py` and `ingestion/cache_enqueuer.py` to publish
3. Cache Layer already accepts HTTP POST — push subscription hits same endpoints
4. Verify, then delete Cloud Tasks queues

**Phase 2: Crawler priority queues** (moderate risk)
1. Create `crawler-authors-priority` and `crawler-pubs-priority` topics
2. Update `frontend/queue_client.py` (author crawl) and `crawler/task_enqueuer.py` (priority pubs)
3. Add application-level dedup in consumer
4. Verify, then delete Cloud Tasks queues

**Phase 3: Crawler standard queues** (highest risk — rate limiting critical)
1. Create `crawler-authors` and `crawler-pubs` topics
2. Convert crawler to Cloud Run service with pull subscribers (if using pull model)
3. Update `refresh/task_enqueuer.py` and `crawler/task_enqueuer.py`
4. Implement flow control for Scholar API rate limiting
5. Verify, then delete Cloud Tasks queues

**Each phase can be deployed independently. Rollback = revert the enqueuer to Cloud Tasks.**

### Files Changed

| File | Change |
|------|--------|
| `crawler/task_enqueuer.py` | Replace Cloud Tasks client with Pub/Sub publisher |
| `refresh/task_enqueuer.py` | Replace Cloud Tasks client with Pub/Sub publisher |
| `frontend/queue_client.py` | Replace Cloud Tasks client with Pub/Sub publisher |
| `ingestion/cache_enqueuer.py` | Replace Cloud Tasks client with Pub/Sub publisher |
| `crawler/dead_letter_handler.py` | Expand to handle all DLQ topics, write to Firestore |
| `crawler/fetch_author.py` | Add application-level dedup (if not using push) |
| `crawler/fetch_publication.py` | Add application-level dedup |
| `cache_layer/main.py` | No change (push subscriptions hit same endpoints) |
| `*/config.py` (all 5) | Replace queue names/locations with topic/subscription paths |
| `*/requirements.txt` (all affected) | Replace `google-cloud-tasks` with `google-cloud-pubsub` |
| `.github/workflows/deploy-infrastructure.yml` | Create topics, subscriptions, DLQ config |
| `.github/workflows/deploy-crawler.yml` | Update function deployment (or Cloud Run if pull model) |
| All test files for above | Mock Pub/Sub instead of Cloud Tasks |

### Pros of Option B
- **Native dead-letter on every subscription** — no silent failures possible
- **Message replay** — can seek back to re-process messages (Cloud Tasks can't do this)
- **Better observability** — Pub/Sub metrics (unacked messages, oldest unacked age, delivery latency) are richer than Cloud Tasks
- **Message filtering** — subscriptions can filter by attributes (e.g., priority, task type)
- **Exactly-once delivery** — available for pull subscriptions (Cloud Tasks is at-least-once)
- **Higher throughput ceiling** — Pub/Sub scales to millions of messages/sec
- **Decoupled producers/consumers** — adding a new consumer doesn't require changing producers
- **Message retention** — up to 31 days (default 7). Cloud Tasks retains nothing after completion.

### Cons of Option B
- **No built-in rate limiting** — Must implement via pull subscriber flow control or Cloud Run concurrency. This is the biggest gap for Scholar API protection.
- **Weaker dedup** — 10-minute dedup window (same as current time-bucket, but less deterministic). Application-level dedup adds complexity.
- **Cost increase** — Pub/Sub charges per message ($0.40/million). At current volume (~1,000 tasks/day) this is negligible (<$1/month), but good to know.
- **Migration effort** — ~2-3 weeks, touching every component. Risk of regression.
- **Pull subscribers require Cloud Run** — Crawler would need to change from Cloud Functions to a long-running Cloud Run service (if using pull model for rate control)
- **Push subscriptions have no rate limit** — Would need to rely on Cloud Run concurrency limits, which are less precise than Cloud Tasks' dispatches/sec
- **More infrastructure** — 6 topics + 6 subscriptions + 6 DLQ topics + 6 DLQ subscriptions = 24 resources (vs 6 queues today)
- **OIDC auth works differently** — Push subscriptions use service account impersonation, slightly different config than Cloud Tasks OIDC

---

## Recommendation

**Start with Option A.** It solves the core problem (silent failures + failure tracking) in ~2-3 days with minimal risk. The key changes:

1. Add dead-letter config to all 6 queues (infrastructure change only)
2. Expand `dead_letter_handler.py` to write failures to Firestore
3. Add failure lifecycle tracking (failed → retrying → resolved)
4. Add `/admin/failures` dashboard

**Then evaluate Option B** based on whether you need:
- Message replay (re-process historical messages)
- Richer observability (Pub/Sub metrics dashboards)
- Decoupled consumers (e.g., multiple services processing the same message)

If none of those are needed, Option A is sufficient and much simpler.

---

## Hybrid Option: Pub/Sub for Observability, Cloud Tasks for Rate Limiting

A pragmatic middle ground:

1. **Producers publish to Pub/Sub topics** (for observability, dead-letter, replay)
2. **A bridge function subscribes to Pub/Sub and creates Cloud Tasks** (for rate limiting, dedup)
3. **Cloud Tasks dispatch to the same Cloud Function endpoints** (no consumer changes)

This gives you Pub/Sub's dead-letter and observability benefits while keeping Cloud Tasks' rate limiting. But it adds latency and complexity (an extra hop), so it's only worth it if you specifically need message replay or multiple consumers.

---

## Implementation Priority

| Step | Option | Effort | Impact |
|------|--------|--------|--------|
| 1. Dead-letter on all queues | A1 | 1 day | Eliminates silent failures |
| 2. Firestore failure tracking | A2 | 1 day | Full visibility into what failed |
| 3. Failure dashboard | A2 | 0.5 day | Operational visibility |
| 4. Track partial pub enqueue failures | A3 | 0.5 day | Catches edge case failures |
| 5. Alerting | A4 | 0.5 day | Proactive notification |
| 6. (Optional) Migrate cache queues to Pub/Sub | B Phase 1 | 2 days | Test Pub/Sub in low-risk path |
| 7. (Optional) Migrate crawler to Pub/Sub | B Phase 2-3 | 1-2 weeks | Full Pub/Sub benefits |
