"""Flask route handlers for the PiP Score frontend.

The frontend reads exclusively from Firestore cache. On cache miss, it
enqueues a population task to the Cache Layer's priority queue and returns
a loading page. Visualization uses Plotly.js on the client side — the
server passes structured JSON data to templates.
"""

import datetime
import io
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait

import pandas as pd
from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from google.cloud import firestore

from frontend.cache import FirestoreCache
from frontend.config import Config
from frontend.queue_client import enqueue_cache_populate

logger = logging.getLogger(__name__)

# Validate author_id: alphanumeric, hyphens, underscores, 1-20 chars.
# Accepts both S2 numeric IDs (e.g., "2942126") and legacy GS IDs.
SCHOLAR_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,20}$")

# Validate author_pub_id: scholar_id:base64-like, up to 60 chars
AUTHOR_PUB_ID_RE = re.compile(r"^[A-Za-z0-9_:/-]{4,80}$")


def _validate_scholar_id(scholar_id):
    if not scholar_id or not SCHOLAR_ID_RE.match(scholar_id):
        return None
    return scholar_id


def _validate_author_pub_id(pub_id):
    if not pub_id or not AUTHOR_PUB_ID_RE.match(pub_id):
        return None
    return pub_id


def _prepare_pub_chart_data(pub_stats, author_stats):
    """Prepare publication stats for client-side Plotly charts.

    Returns a list of dicts with fields needed by the scatter plots.
    """
    if not pub_stats:
        return []
    current_year = datetime.datetime.now().year
    result = []
    for p in pub_stats:
        pub_year = p.get("pub_year")
        try:
            pub_year = int(pub_year)
        except (TypeError, ValueError):
            continue
        result.append({
            "publication_rank": p.get("publication_rank"),
            "num_citations_percentile": round(100 * (p.get("num_citations_percentile") or 0), 2),
            "num_papers_percentile": round(100 * (p.get("num_papers_percentile") or 0), 2),
            "age": current_year - pub_year + 1,
            "title": p.get("title", ""),
            "num_citations": p.get("num_citations", 0),
            "pub_year": pub_year,
        })
    return result


def _prepare_temporal_chart_data(temporal_stats, author_stats):
    """Prepare temporal stats for client-side Plotly charts.

    Filters out bogus early years and returns cleaned list of dicts.
    """
    if not temporal_stats:
        return []
    year_of_first_pub = author_stats.get("year_of_first_pub") if author_stats else None
    result = []
    for t in temporal_stats:
        state_year = t.get("state_year")
        try:
            state_year = int(state_year)
        except (TypeError, ValueError):
            continue
        if state_year <= 1950:
            continue
        if year_of_first_pub is not None and state_year < int(year_of_first_pub):
            continue
        result.append({
            "state_year": state_year,
            "h_index": t.get("h_index"),
            "h_index_percentile": round(100 * (t.get("h_index_percentile") or 0), 2),
            "total_citations": t.get("total_citations"),
            "total_citations_percentile": round(100 * (t.get("total_citations_percentile") or 0), 2),
            "i10_index": t.get("i10_index"),
            "i10_index_percentile": round(100 * (t.get("i10_index_percentile") or 0), 2),
        })
    return result


_CACHE_READ_TIMEOUT = 10  # seconds; prevent hanging on slow Firestore reads

# Shared bounded thread pool for cache reads.  A fixed pool (rather than
# per-request executors) caps the total number of threads that can be
# occupied by hung Firestore reads, preventing thread leaks under partial
# backend outages.
_cache_read_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="cache-read")


def _parallel_cache_reads(read_cache_fn, reads):
    """Run cache reads in parallel with a single overall timeout.

    Args:
        read_cache_fn: callable(collection, doc_id) -> data or None
        reads: list of (collection, doc_id) tuples

    Returns list of results (data or None) in the same order as reads.
    Uses a single deadline for all futures so total wall time is bounded
    to _CACHE_READ_TIMEOUT regardless of how many reads are in flight.
    Completed reads are returned even if some time out.
    """
    futures = [_cache_read_pool.submit(read_cache_fn, col, doc) for col, doc in reads]

    done, not_done = futures_wait(futures, timeout=_CACHE_READ_TIMEOUT)

    for f in not_done:
        f.cancel()

    return [f.result() if f in done else None for f in futures]


def register_routes(app):
    cache = FirestoreCache()

    def _read_cache(collection, doc_id):
        """Read from Firestore cache. Returns data or None."""
        return cache.get(collection, doc_id)

    @app.route("/")
    @app.route("/index")
    def index():
        recent_authors = _read_cache("v3_recent_authors", "recent")
        return render_template("index.html", recent_authors=recent_authors or [])

    def _get_author_freshness(author_id):
        """Read cached freshness for an author.

        Returns (exists: bool, last_updated: datetime|None).
        """
        cached = _read_cache(Config.CACHE_AUTHOR_FRESHNESS, author_id)
        if cached is not None:
            return cached.get("exists", True), cached.get("last_updated")
        return None, None

    @app.route("/results")
    def results():
        author_id = _validate_scholar_id(request.args.get("author_id", "").strip())
        if not author_id:
            flash("A valid author ID is required.")
            return redirect(url_for("index"))

        # Check cached freshness
        exists, last_updated = _get_author_freshness(author_id)

        # If freshness not cached, enqueue cache population and show loading
        if exists is None:
            cache_enqueued = enqueue_cache_populate(
                "populate_author_profile", {"scholar_id": author_id},
            )
            has_cached_data = _read_cache(
                Config.CACHE_AUTHOR_STATS, author_id,
            ) is not None
            return render_template(
                "redirect.html",
                author_id=author_id,
                status="unknown",
                cache_enqueued=cache_enqueued,
                has_cached_data=has_cached_data,
            )

        if not exists:
            return render_template(
                "redirect.html",
                author_id=author_id,
                status="not_found",
            )

        # Read all data from cache (parallel)
        author_stats, temporal_stats, pub_stats = _parallel_cache_reads(
            _read_cache, [
                (Config.CACHE_AUTHOR_STATS, author_id),
                (Config.CACHE_AUTHOR_TEMPORAL, author_id),
                (Config.CACHE_AUTHOR_PUB_STATS, author_id),
            ],
        )

        if not author_stats:
            # Cache miss — enqueue population and show loading page
            cache_enqueued = enqueue_cache_populate(
                "populate_author_profile", {"scholar_id": author_id},
            )
            return render_template(
                "loading.html", author_id=author_id,
                cache_enqueued=cache_enqueued,
            )

        # Prepare chart data for client-side Plotly rendering
        pub_chart_data = _prepare_pub_chart_data(pub_stats, author_stats)
        temporal_chart_data = _prepare_temporal_chart_data(temporal_stats, author_stats)

        # Read statistics cache timestamp (when stats were last computed)
        stats_cached_at = cache.get_timestamp(Config.CACHE_AUTHOR_STATS, author_id)

        # Record this author as recently queried (fire-and-forget)
        try:
            cache.record_recent_author(author_stats)
        except Exception:
            logger.debug("Failed to record recent author %s", author_id)

        # Log profile view
        try:
            cache.log_query(
                "profile_view",
                author_stats.get("name", author_id),
                author_id=author_id,
            )
        except Exception:
            logger.debug("Failed to log profile view for %s", author_id)

        return render_template(
            "results.html",
            author_id=author_id,
            stats=author_stats,
            pub_chart_data=pub_chart_data,
            temporal_chart_data=temporal_chart_data,
            last_updated=last_updated,
            stats_cached_at=stats_cached_at,
        )

    @app.route("/publications/<author_id>")
    def publications(author_id):
        author_id = _validate_scholar_id(author_id)
        if not author_id:
            flash("A valid author ID is required.")
            return redirect(url_for("index"))

        exists, last_updated = _get_author_freshness(author_id)
        if not exists:
            flash("Author not found.")
            return redirect(url_for("index"))

        author_stats, pub_stats = _parallel_cache_reads(
            _read_cache, [
                (Config.CACHE_AUTHOR_STATS, author_id),
                (Config.CACHE_AUTHOR_PUB_STATS, author_id),
            ],
        )

        if not author_stats:
            cache_enqueued = enqueue_cache_populate("populate_author_profile", {"scholar_id": author_id})
            return render_template("loading.html", author_id=author_id, cache_enqueued=cache_enqueued)

        return render_template(
            "publications.html",
            author_id=author_id,
            stats=author_stats,
            publications=pub_stats or [],
            last_updated=last_updated,
        )

    @app.route("/publication/<author_id>/<path:pub_id>")
    def publication_details(author_id, pub_id):
        author_id = _validate_scholar_id(author_id)
        pub_id = _validate_author_pub_id(pub_id)
        if not author_id or not pub_id:
            return render_template("error.html", error_message="Invalid parameters.")

        pub_stats = _read_cache(Config.CACHE_PUB_STATS, pub_id)

        if not pub_stats:
            cache_enqueued = enqueue_cache_populate("populate_publication_detail", {"author_pub_id": pub_id})
            return render_template("loading.html", author_id=author_id, cache_enqueued=cache_enqueued)

        return render_template(
            "publication_details.html",
            author_id=author_id,
            pub_id=pub_id,
            pub_stats=pub_stats,
        )

    @app.route("/download/<author_id>")
    def download_results(author_id):
        author_id = _validate_scholar_id(author_id)
        if not author_id:
            flash("Invalid author ID.")
            return redirect(url_for("index"))

        # Read pub_stats from cache
        pub_stats = _read_cache(Config.CACHE_AUTHOR_PUB_STATS, author_id)
        if not pub_stats:
            flash("No publications found to download. Please visit the author profile first.")
            return redirect(url_for("index"))

        df = pd.DataFrame(pub_stats)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{author_id}_results.csv",
        )

    @app.route("/data")
    def data():
        return render_template("data.html")

    @app.route("/help")
    def help_page():
        return render_template("help.html")

    # ------------------------------------------------------------------
    # JSON API endpoints for chart data (programmatic access)
    # ------------------------------------------------------------------

    @app.route("/api/author/<author_id>/data")
    def api_author_data(author_id):
        """Return all author data as JSON for programmatic access.

        Includes author stats, publication chart data, and temporal chart data.
        """
        author_id = _validate_scholar_id(author_id)
        if not author_id:
            return jsonify({"error": "Invalid author ID"}), 400

        # Check freshness first (same as /results) to avoid queue churn
        exists, _ = _get_author_freshness(author_id)
        if exists is None:
            enqueue_cache_populate("populate_author_profile", {"scholar_id": author_id})
            return jsonify({"error": "Data not ready", "status": "loading"}), 202
        if not exists:
            return jsonify({"error": "Author not found", "status": "not_found"}), 404

        author_stats, temporal_stats, pub_stats = _parallel_cache_reads(
            _read_cache, [
                (Config.CACHE_AUTHOR_STATS, author_id),
                (Config.CACHE_AUTHOR_TEMPORAL, author_id),
                (Config.CACHE_AUTHOR_PUB_STATS, author_id),
            ],
        )

        if not author_stats:
            enqueue_cache_populate("populate_author_profile", {"scholar_id": author_id})
            return jsonify({"error": "Data not ready", "status": "loading"}), 202

        return jsonify({
            "author": author_stats,
            "publications": _prepare_pub_chart_data(pub_stats, author_stats),
            "temporal": _prepare_temporal_chart_data(temporal_stats, author_stats),
        })

    @app.route("/api/publication/<path:pub_id>/data")
    def api_publication_data(pub_id):
        """Return publication citation data as JSON."""
        pub_id = _validate_author_pub_id(pub_id)
        if not pub_id:
            return jsonify({"error": "Invalid publication ID"}), 400

        pub_stats = _read_cache(Config.CACHE_PUB_STATS, pub_id)
        if not pub_stats:
            enqueue_cache_populate("populate_publication_detail", {"author_pub_id": pub_id})
            return jsonify({"error": "Data not ready", "status": "loading"}), 202

        return jsonify({"citations": pub_stats})

    # ------------------------------------------------------------------
    # Existing API endpoints
    # ------------------------------------------------------------------

    # Simple per-author rate limiting for state-changing endpoints.
    # Tracks recent rebuild requests to prevent duplicate enqueues.
    import threading
    _rebuild_timestamps = {}  # author_id -> monotonic timestamp
    _rebuild_lock = threading.Lock()
    _REBUILD_COOLDOWN = 60  # seconds between rebuilds for the same author

    @app.route("/api/rebuild_statistics", methods=["POST"])
    def api_rebuild_statistics():
        """Enqueue cache rebuild from BigQuery for the given author IDs."""
        scholar_ids_arg = request.form.get("scholar_ids", "") or request.args.get("scholar_ids", "")
        scholar_ids = [s.strip() for s in scholar_ids_arg.split(",")
                       if _validate_scholar_id(s.strip())]
        if not scholar_ids:
            return jsonify({"error": "No valid author IDs provided"}), 400

        now = time.monotonic()
        with _rebuild_lock:
            # Evict expired entries
            expired = [k for k, v in _rebuild_timestamps.items() if now - v >= _REBUILD_COOLDOWN]
            for k in expired:
                del _rebuild_timestamps[k]
            # Filter to authors not on cooldown, and optimistically
            # mark them to prevent concurrent duplicate enqueues.
            to_enqueue = []
            for sid in scholar_ids:
                if now - _rebuild_timestamps.get(sid, 0) >= _REBUILD_COOLDOWN:
                    _rebuild_timestamps[sid] = now
                    to_enqueue.append(sid)

        enqueued = 0
        skipped = len(scholar_ids) - len(to_enqueue)
        for sid in to_enqueue:
            if enqueue_cache_populate("populate_author_profile", {"scholar_id": sid}):
                enqueued += 1
            else:
                # Enqueue failed — remove cooldown so retries aren't blocked
                with _rebuild_lock:
                    _rebuild_timestamps.pop(sid, None)
        return jsonify({
            "status": "queued",
            "total_authors": len(scholar_ids),
            "enqueued": enqueued,
            "skipped_rate_limited": skipped,
            "authors": [{"scholar_id": sid} for sid in scholar_ids],
        })

    from author_search.search_service import AuthorSearchService, refresh_author_index
    search_svc = AuthorSearchService()

    @app.route("/get_similar_authors")
    def get_similar_authors():
        author_name = request.args.get("author_name", "").strip()
        if not author_name or len(author_name) < 2:
            return jsonify([])
        typeahead = request.args.get("typeahead", "").lower() == "true"
        scholar = request.args.get("scholar", "").lower() == "true"
        results = search_svc.search(author_name, typeahead=typeahead, scholar=scholar)

        # Log non-typeahead searches (typeahead fires on every keystroke)
        if not typeahead:
            try:
                cache.log_query(
                    "search", author_name,
                    result_count=len(results),
                    typeahead=typeahead, scholar=scholar,
                )
            except Exception:
                logger.debug("Failed to log search query")

        return jsonify(results)

    @app.route("/api/refresh_author_index", methods=["POST"])
    def api_refresh_author_index():
        """Rebuild the in-memory author search index from BigQuery."""
        count = refresh_author_index(bq=search_svc.bq, cache=search_svc.cache)
        return jsonify({"status": "ok", "authors_indexed": count})

    # ------------------------------------------------------------------
    # Task Failures Dashboard
    # ------------------------------------------------------------------

    @app.route("/admin/failures")
    def admin_failures():
        """Dashboard showing tasks that exhausted all retries."""
        show_resolved = request.args.get("show_resolved", "").lower() == "true"
        failures = _get_task_failures(show_resolved=show_resolved)
        return render_template("failures.html", failures=failures)

    @app.route("/api/failures")
    def api_failures():
        """JSON API for task failure data."""
        show_resolved = request.args.get("show_resolved", "").lower() == "true"
        failures = _get_task_failures(show_resolved=show_resolved)
        return jsonify(failures)

    def _get_task_failures(show_resolved=False):
        """Query Firestore task_failures collection.

        Avoids requiring a composite Firestore index by filtering and
        sorting client-side. The collection is small (only dead-lettered
        tasks), so this is fine.
        """
        try:
            db = cache.db
            docs = db.collection("task_failures").stream()
            results = []
            for doc in docs:
                data = doc.to_dict()
                if not show_resolved and data.get("status") == "resolved":
                    continue
                results.append(data)
            results.sort(key=lambda d: d.get("last_failure", ""), reverse=True)
            return results[:100]
        except Exception:
            logger.exception("Failed to query task failures")
            return []

    # ------------------------------------------------------------------
    # Query Log
    # ------------------------------------------------------------------

    def _parse_limit(raw, default=200, maximum=1000):
        try:
            return max(1, min(int(raw), maximum))
        except (ValueError, TypeError):
            return default

    @app.route("/admin/query-log")
    def admin_query_log():
        """View recent search queries and profile views."""
        query_type = request.args.get("type", "")  # 'search', 'profile_view', or '' for all
        limit = _parse_limit(request.args.get("limit", "200"))
        queries = _get_query_log(query_type=query_type, limit=limit)
        return render_template("query_log.html", queries=queries, query_type=query_type)

    @app.route("/api/query-log")
    def api_query_log():
        """JSON API for query log data."""
        query_type = request.args.get("type", "")
        limit = _parse_limit(request.args.get("limit", "200"))
        queries = _get_query_log(query_type=query_type, limit=limit)
        return jsonify(queries)

    def _get_query_log(query_type="", limit=200):
        """Read recent queries from Firestore query log.

        Filtering by type is done client-side to avoid requiring a
        Firestore composite index (order_by + where on different fields).
        """
        try:
            db = cache.db
            ref = db.collection(Config.CACHE_QUERY_LOG)
            ref = ref.order_by("timestamp", direction=firestore.Query.DESCENDING)
            # Fetch extra when filtering so we're likely to fill the limit
            fetch_limit = limit * 3 if query_type else limit
            ref = ref.limit(fetch_limit)
            results = []
            for doc in ref.stream():
                data = doc.to_dict()
                if query_type and data.get("type") != query_type:
                    continue
                ts = data.get("timestamp")
                if ts:
                    data["timestamp"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                results.append(data)
                if len(results) >= limit:
                    break
            return results
        except Exception:
            logger.exception("Failed to query log")
            return []

    # ------------------------------------------------------------------
    # Health Dashboard
    # ------------------------------------------------------------------

    @app.route("/health-dashboard")
    def health_dashboard():
        """System health dashboard — simplified for S2 data pipeline."""
        return render_template("health.html", data={})

    @app.route("/api/health")
    def api_health():
        """JSON API for health status."""
        return jsonify({"status": "ok"})

    @app.route("/api/speed_check")
    def api_speed_check():
        """Verify that the cached results path is fast.

        Hits the full data pipeline (freshness, author stats, pub stats,
        temporal stats) for a known author using cache reads only, and
        reports per-step timings.
        """
        default_author = os.environ.get("SPEED_CHECK_AUTHOR_ID", "2942126")
        author_id = _validate_scholar_id(
            request.args.get("author_id", default_author).strip()
        )
        try:
            threshold = float(request.args.get("threshold", "5"))
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Invalid threshold parameter"}), 400

        if not author_id:
            return jsonify({"status": "error", "message": "Invalid author_id"}), 400

        timings = {}

        # 1. Freshness check
        t0 = time.monotonic()
        exists, last_updated = _get_author_freshness(author_id)
        timings["freshness_ms"] = round((time.monotonic() - t0) * 1000)

        if not exists:
            return jsonify({
                "status": "error",
                "message": f"Author {author_id} not found — pick an author already in the database",
                "timings": timings,
            }), 404

        # 2. Author stats
        t0 = time.monotonic()
        author_stats = _read_cache(Config.CACHE_AUTHOR_STATS, author_id)
        timings["author_stats_ms"] = round((time.monotonic() - t0) * 1000)

        # 3. Publication stats
        t0 = time.monotonic()
        pub_stats = _read_cache(Config.CACHE_AUTHOR_PUB_STATS, author_id)
        timings["pub_stats_ms"] = round((time.monotonic() - t0) * 1000)

        # 4. Temporal stats
        t0 = time.monotonic()
        temporal_stats = _read_cache(Config.CACHE_AUTHOR_TEMPORAL, author_id)
        timings["temporal_stats_ms"] = round((time.monotonic() - t0) * 1000)

        total_s = sum(timings.values()) / 1000
        timings["total_ms"] = round(total_s * 1000)

        ok = total_s <= threshold
        result = {
            "status": "ok" if ok else "slow",
            "total_seconds": round(total_s, 2),
            "threshold_seconds": threshold,
            "author_id": author_id,
            "has_author_stats": author_stats is not None,
            "has_pub_stats": bool(pub_stats),
            "has_temporal_stats": bool(temporal_stats),
            "timings": timings,
        }
        return jsonify(result), 200

    @app.errorhandler(404)
    def not_found(e):
        return render_template("error.html", error_message="Page not found."), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template("error.html", error_message="Internal server error."), 500
