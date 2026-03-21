"""Flask route handlers for the Scholar Analytics frontend.

The frontend reads exclusively from Firestore cache. On cache miss, it
enqueues a population task to the Cache Layer's priority queue and returns
a loading page. Visualization (matplotlib) runs in the frontend from
cached data.
"""

import datetime
import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, timezone

import pandas as pd
import urllib.request
from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from frontend.cache import FirestoreCache
from frontend.config import Config
from frontend.health_service import HealthService
from frontend.queue_client import enqueue_author_crawl, enqueue_cache_populate
from frontend.visualization import (
    generate_percentile_rank_plot,
    generate_pip_plot,
    generate_pub_citation_plot,
    generate_author_h_index_plot,
    generate_author_total_citations_plot,
    generate_author_i10_index_plot,
    generate_author_h_index_5y_plot,
)

logger = logging.getLogger(__name__)

# Validate scholar_id: alphanumeric, hyphens, underscores, 4-20 chars
SCHOLAR_ID_RE = re.compile(r"^[A-Za-z0-9_-]{4,20}$")

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


def _call_refresh_function(function_name, params=None, body=None, timeout=10):
    """Call a Refresh Cloud Function by name.

    Returns the parsed JSON response, or None if not configured or the call fails.
    """
    base_url = Config.REFRESH_FUNCTIONS_BASE
    if not base_url:
        return None

    url = f"{base_url.rstrip('/')}/{function_name}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"

    try:
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"} if data else {},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        logger.exception("Refresh function call failed: %s", function_name)
        return None


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

    def _generate_all_plots(author_stats, pub_stats, temporal_stats):
        """Generate all plots for an author and return as a cacheable dict."""
        plot1, plot2 = "", ""
        if pub_stats:
            df = pd.DataFrame(pub_stats)
            author_name = author_stats.get("name", "N/A")
            current_year = datetime.datetime.now().year
            df["pub_year"] = pd.to_numeric(df["pub_year"], errors="coerce")
            df.dropna(subset=["pub_year"], inplace=True)
            df["pub_year"] = df["pub_year"].astype(int)
            df["age"] = current_year - df["pub_year"] + 1
            df["num_citations_percentile"] = 100 * df["num_citations_percentile"]
            df["num_papers_percentile"] = 100 * df["num_papers_percentile"]
            with ThreadPoolExecutor(max_workers=2) as executor:
                plot1_future = executor.submit(generate_percentile_rank_plot, df.copy(), author_name)
                plot2_future = executor.submit(generate_pip_plot, df.copy(), author_name)
                plot1 = plot1_future.result()
                plot2 = plot2_future.result()

        temporal_plots = {}
        if temporal_stats:
            tdf = pd.DataFrame(temporal_stats)
            tdf["state_year"] = pd.to_numeric(tdf["state_year"], errors="coerce")
            tdf.dropna(subset=["state_year"], inplace=True)
            # Filter out bogus early years (e.g., pub_year=1800 from Scholar data)
            tdf = tdf[tdf["state_year"] > 1950]
            if not tdf.empty:
                temporal_plots["h_index"] = generate_author_h_index_plot(tdf)
                temporal_plots["total_citations"] = generate_author_total_citations_plot(tdf)
                temporal_plots["i10_index"] = generate_author_i10_index_plot(tdf)
                temporal_plots["h_index_5y"] = generate_author_h_index_5y_plot(tdf)

        return {"plot1": plot1, "plot2": plot2, "temporal_plots": temporal_plots}

    @app.route("/results")
    def results():
        author_id = _validate_scholar_id(request.args.get("author_id", "").strip())
        if not author_id:
            flash("A valid Google Scholar ID is required.")
            return redirect(url_for("index"))

        # Check cached freshness
        exists, last_updated = _get_author_freshness(author_id)

        # If freshness not cached, enqueue crawl + cache population and show loading
        if exists is None:
            cache_enqueued = enqueue_cache_populate(
                "populate_author_profile", {"scholar_id": author_id},
            )
            # Enqueue author crawl directly to priority queue
            crawl_enqueued = enqueue_author_crawl(author_id)
            # Check Firestore for existing data to determine if new or known
            has_cached_data = _read_cache(
                Config.CACHE_AUTHOR_STATS, author_id,
            ) is not None
            return render_template(
                "redirect.html",
                author_id=author_id,
                status="unknown",
                cache_enqueued=cache_enqueued,
                refresh_result={"enqueued": crawl_enqueued},
                has_cached_data=has_cached_data,
            )

        if not exists:
            crawl_enqueued = enqueue_author_crawl(author_id)
            return render_template(
                "redirect.html",
                author_id=author_id,
                status="not_found",
                cache_enqueued=False,
                refresh_result={"enqueued": crawl_enqueued},
            )

        # Read all data from cache
        cached_plots = _read_cache("v3_author_plots", author_id)

        with ThreadPoolExecutor(max_workers=3) as executor:
            author_stats_future = executor.submit(
                _read_cache, Config.CACHE_AUTHOR_STATS, author_id,
            )
            temporal_stats_future = executor.submit(
                _read_cache, Config.CACHE_AUTHOR_TEMPORAL, author_id,
            )
            pub_stats_future = None
            if cached_plots is None:
                pub_stats_future = executor.submit(
                    _read_cache, Config.CACHE_AUTHOR_PUB_STATS, author_id,
                )

            author_stats = author_stats_future.result()
            temporal_stats = temporal_stats_future.result()
            pub_stats = pub_stats_future.result() if pub_stats_future else None

        if not author_stats:
            # Cache miss — enqueue population and show loading page
            cache_enqueued = enqueue_cache_populate(
                "populate_author_profile", {"scholar_id": author_id},
            )
            return render_template(
                "loading.html", author_id=author_id,
                cache_enqueued=cache_enqueued,
            )

        # Generate plots from cached data (visualization stays in frontend)
        if cached_plots is None:
            cached_plots = _generate_all_plots(author_stats, pub_stats, temporal_stats)
            # Cache the plots in Firestore for next time
            cache.set("v3_author_plots", author_id, cached_plots)

        # Read statistics cache timestamp (when stats were last computed)
        stats_cached_at = cache.get_timestamp(Config.CACHE_AUTHOR_STATS, author_id)

        # Record this author as recently queried (fire-and-forget)
        try:
            cache.record_recent_author(author_stats)
        except Exception:
            logger.debug("Failed to record recent author %s", author_id)

        return render_template(
            "results.html",
            author_id=author_id,
            stats=author_stats,
            plot1=cached_plots.get("plot1", ""),
            plot2=cached_plots.get("plot2", ""),
            temporal_plots=cached_plots.get("temporal_plots", {}),
            last_updated=last_updated,
            stats_cached_at=stats_cached_at,
        )

    @app.route("/publications/<author_id>")
    def publications(author_id):
        author_id = _validate_scholar_id(author_id)
        if not author_id:
            flash("A valid Google Scholar ID is required.")
            return redirect(url_for("index"))

        exists, last_updated = _get_author_freshness(author_id)
        if not exists:
            flash("Author not found.")
            return redirect(url_for("index"))

        with ThreadPoolExecutor(max_workers=2) as executor:
            author_stats_future = executor.submit(
                _read_cache, Config.CACHE_AUTHOR_STATS, author_id,
            )
            pub_stats_future = executor.submit(
                _read_cache, Config.CACHE_AUTHOR_PUB_STATS, author_id,
            )
            author_stats = author_stats_future.result()
            pub_stats = pub_stats_future.result()

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

        citations_plot = generate_pub_citation_plot(pd.DataFrame(pub_stats))

        return render_template(
            "publication_details.html",
            author_id=author_id,
            pub_id=pub_id,
            pub_stats=pub_stats,
            citations_plot=citations_plot,
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

    @app.route("/api/fetch_authors")
    def api_fetch_authors():
        scholar_ids_arg = request.args.get("scholar_ids", "")
        scholar_ids = [s.strip() for s in scholar_ids_arg.split(",")
                       if _validate_scholar_id(s.strip())]
        if not scholar_ids:
            return jsonify({"error": "No valid scholar IDs provided"}), 400
        enqueued = 0
        for sid in scholar_ids:
            if enqueue_author_crawl(sid):
                enqueued += 1
        return jsonify({
            "status": "queued",
            "total_authors": len(scholar_ids),
            "enqueued": enqueued,
            "authors": [{"scholar_id": sid} for sid in scholar_ids],
        })

    @app.route("/api/rebuild_statistics")
    def api_rebuild_statistics():
        scholar_ids_arg = request.args.get("scholar_ids", "")
        scholar_ids = [s.strip() for s in scholar_ids_arg.split(",")
                       if _validate_scholar_id(s.strip())]
        if not scholar_ids:
            return jsonify({"error": "No valid scholar IDs provided"}), 400
        for sid in scholar_ids:
            enqueue_cache_populate("populate_author_profile", {"scholar_id": sid})
            # Invalidate cached plots so they are regenerated from fresh stats
            cache.delete("v3_author_plots", sid)
        return jsonify({
            "status": "queued",
            "total_authors": len(scholar_ids),
            "authors": [{"scholar_id": sid} for sid in scholar_ids],
        })

    @app.route("/api/refresh_stale_authors")
    def api_refresh_stale():
        num = request.args.get("num_authors", "5")
        result = _call_refresh_function("v3_refresh_stale", params={"limit": num})
        if result is not None:
            return jsonify(result)
        return jsonify({"status": "not_configured", "message": "Refresh functions not yet configured"})

    @app.route("/api/add_coauthors")
    def api_add_coauthors():
        num = request.args.get("num_authors", "1")
        result = _call_refresh_function("v3_expand_coauthors", params={"limit": num})
        if result is not None:
            return jsonify(result)
        return jsonify({"status": "not_configured", "message": "Refresh functions not yet configured"})

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
        return jsonify(results)

    @app.route("/api/refresh_author_index")
    def api_refresh_author_index():
        count = refresh_author_index(bq=search_svc.bq, cache=search_svc.cache)
        return jsonify({"status": "ok", "authors_indexed": count})

    # ------------------------------------------------------------------
    # Health Dashboard
    # ------------------------------------------------------------------
    health_service = HealthService()

    @app.route("/health-dashboard")
    def health_dashboard():
        """System health dashboard showing queue status, data freshness, and errors."""
        data = health_service.get_dashboard_data()
        return render_template("health.html", data=data)

    @app.route("/api/health")
    def api_health():
        """JSON API for health dashboard data."""
        return jsonify(health_service.get_dashboard_data())

    @app.route("/api/speed_check")
    def api_speed_check():
        """Verify that the cached results path is fast.

        Hits the full data pipeline (freshness, author stats, pub stats,
        temporal stats) for a known author using cache reads only, and
        reports per-step timings.
        """
        default_author = os.environ.get("SPEED_CHECK_AUTHOR_ID", "evnr-MwAAAAJ")
        author_id = _validate_scholar_id(
            request.args.get("author_id", default_author).strip()
        )
        threshold = float(request.args.get("threshold", "5"))

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
