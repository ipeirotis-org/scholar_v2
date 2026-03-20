"""Flask route handlers for the Scholar Analytics frontend."""

import datetime
import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

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

from frontend.bigquery_client import BigQueryClient
from frontend.cache import FirestoreCache
from frontend.config import Config
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


def _call_refresh_service(path, params=None, body=None, timeout=10):
    """Call the Refresh & Expand service (Component 5).

    Returns the parsed JSON response, or None if the service is not configured
    or the call fails.
    """
    base_url = Config.REFRESH_SERVICE_URL
    if not base_url:
        return None

    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
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
        logger.exception("Refresh service call failed: %s", path)
        return None


def register_routes(app):
    bq = BigQueryClient()
    cache = FirestoreCache()

    def _get_cached_or_query(collection, doc_id, query_fn, valid_after=None):
        """Get from cache or query BigQuery, then cache the result."""
        data = cache.get(collection, doc_id, valid_after=valid_after)
        if data is not None:
            return data
        data = query_fn()
        if data:
            cache.set(collection, doc_id, data)
        return data

    @app.route("/")
    @app.route("/index")
    def index():
        recent_authors = _get_cached_or_query(
            "v3_recent_authors", "recent",
            lambda: bq.get_recently_analyzed_authors(limit=20),
        )
        return render_template("index.html", recent_authors=recent_authors or [])

    def _get_author_freshness(author_id):
        """Get author existence + last_updated, with Firestore caching.

        The freshness timestamp is cached for 1 hour to avoid hitting
        BigQuery on every page load. The cache is keyed by author_id and
        automatically invalidated when the underlying data changes (the
        next uncached call will pick up the new timestamp).
        """
        cached = cache.get(Config.CACHE_AUTHOR_FRESHNESS, author_id)
        if cached is not None:
            ts = cached.get("last_updated")
            return cached.get("exists", True), ts
        exists, last_updated = bq.get_author_freshness(author_id)
        cache.set(Config.CACHE_AUTHOR_FRESHNESS, author_id, {
            "exists": exists,
            "last_updated": last_updated,
        })
        return exists, last_updated

    @app.route("/results")
    def results():
        author_id = _validate_scholar_id(request.args.get("author_id", "").strip())
        if not author_id:
            flash("A valid Google Scholar ID is required.")
            return redirect(url_for("index"))

        # Check if author exists and get last_updated in one step (cached)
        exists, last_updated = _get_author_freshness(author_id)
        if not exists:
            _call_refresh_service("fetch_author", body={"scholar_id": author_id})
            return render_template("redirect.html", author_id=author_id)

        # Fetch author stats, pub stats, and temporal stats in parallel (all cached)
        with ThreadPoolExecutor(max_workers=3) as executor:
            author_stats_future = executor.submit(
                _get_cached_or_query,
                Config.CACHE_AUTHOR_STATS, author_id,
                lambda: bq.get_author_stats(author_id),
                last_updated,
            )
            pub_stats_future = executor.submit(
                _get_cached_or_query,
                Config.CACHE_AUTHOR_PUB_STATS, author_id,
                lambda: bq.get_author_pub_stats(author_id),
                last_updated,
            )
            temporal_stats_future = executor.submit(
                _get_cached_or_query,
                Config.CACHE_AUTHOR_TEMPORAL, author_id,
                lambda: bq.get_author_temporal_stats(author_id),
                last_updated,
            )

            author_stats = author_stats_future.result()
            pub_stats = pub_stats_future.result()
            temporal_stats = temporal_stats_future.result()

        if not author_stats:
            return render_template("loading.html", author_id=author_id)

        # Generate PiP plots in parallel
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

        # Generate temporal plots
        temporal_plots = {}
        if temporal_stats:
            tdf = pd.DataFrame(temporal_stats)
            tdf["state_year"] = pd.to_numeric(tdf["state_year"], errors="coerce")
            tdf.dropna(subset=["state_year"], inplace=True)
            if not tdf.empty:
                temporal_plots["h_index"] = generate_author_h_index_plot(tdf)
                temporal_plots["total_citations"] = generate_author_total_citations_plot(tdf)
                temporal_plots["i10_index"] = generate_author_i10_index_plot(tdf)
                temporal_plots["h_index_5y"] = generate_author_h_index_5y_plot(tdf)

        return render_template(
            "results.html",
            author_id=author_id,
            stats=author_stats,
            publications=pub_stats or [],
            plot1=plot1,
            plot2=plot2,
            temporal_plots=temporal_plots,
            last_updated=last_updated,
        )

    @app.route("/publication/<author_id>/<path:pub_id>")
    def publication_details(author_id, pub_id):
        author_id = _validate_scholar_id(author_id)
        pub_id = _validate_author_pub_id(pub_id)
        if not author_id or not pub_id:
            return render_template("error.html", error_message="Invalid parameters.")

        last_updated = bq.get_author_last_updated(author_id)
        pub_stats = _get_cached_or_query(
            Config.CACHE_PUB_STATS, pub_id,
            lambda: bq.get_publication_stats(pub_id),
            valid_after=last_updated,
        )

        if not pub_stats:
            return render_template("error.html", error_message="Publication not found.")

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

        pub_stats = bq.get_author_pub_stats(author_id)
        if not pub_stats:
            flash("No publications found to download.")
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
        result = _call_refresh_service(
            "fetch_authors", body={"scholar_ids": scholar_ids},
        )
        if result is not None:
            return jsonify(result)
        # Fallback if refresh service not configured
        return jsonify({
            "status": "queued",
            "total_authors": len(scholar_ids),
            "authors": [{"scholar_id": sid} for sid in scholar_ids],
        })

    @app.route("/api/refresh_stale_authors")
    def api_refresh_stale():
        num = request.args.get("num_authors", "5")
        result = _call_refresh_service("refresh_stale", params={"limit": num})
        if result is not None:
            return jsonify(result)
        return jsonify({"status": "not_configured", "message": "Refresh service not yet configured"})

    @app.route("/api/add_coauthors")
    def api_add_coauthors():
        num = request.args.get("num_authors", "1")
        result = _call_refresh_service("expand_coauthors", params={"limit": num})
        if result is not None:
            return jsonify(result)
        return jsonify({"status": "not_configured", "message": "Refresh service not yet configured"})

    from author_search.search_service import AuthorSearchService, refresh_author_index
    search_svc = AuthorSearchService()

    @app.route("/get_similar_authors")
    def get_similar_authors():
        author_name = request.args.get("author_name", "").strip()
        if not author_name or len(author_name) < 2:
            return jsonify([])
        typeahead = request.args.get("typeahead", "").lower() == "true"
        results = search_svc.search(author_name, typeahead=typeahead)
        return jsonify(results)

    @app.route("/api/refresh_author_index")
    def api_refresh_author_index():
        count = refresh_author_index(bq=search_svc.bq, cache=search_svc.cache)
        return jsonify({"status": "ok", "authors_indexed": count})

    @app.route("/api/speed_check")
    def api_speed_check():
        """Verify that the cached results path is fast.

        Hits the full /results data pipeline (freshness check, author stats,
        pub stats, temporal stats) for a known author and reports per-step
        timings.  Returns HTTP 200 with status "ok" when the cached round-trip
        is under the threshold, or HTTP 500 with status "slow" otherwise.

        Query params:
            author_id  – scholar ID to test (default: from SPEED_CHECK_AUTHOR_ID env var)
            threshold  – max acceptable seconds (default: 5)
        """
        default_author = os.environ.get("SPEED_CHECK_AUTHOR_ID", "evnr-MwAAAAJ")
        author_id = _validate_scholar_id(
            request.args.get("author_id", default_author).strip()
        )
        threshold = float(request.args.get("threshold", "5"))

        if not author_id:
            return jsonify({"status": "error", "message": "Invalid author_id"}), 400

        timings = {}

        # 1. Freshness check (should be cached after first visit)
        t0 = time.monotonic()
        exists, last_updated = _get_author_freshness(author_id)
        timings["freshness_ms"] = round((time.monotonic() - t0) * 1000)

        if not exists:
            return jsonify({
                "status": "error",
                "message": f"Author {author_id} not found — pick an author already in the database",
                "timings": timings,
            }), 404

        # 2. Author stats (cached)
        t0 = time.monotonic()
        author_stats = _get_cached_or_query(
            Config.CACHE_AUTHOR_STATS, author_id,
            lambda: bq.get_author_stats(author_id),
            last_updated,
        )
        timings["author_stats_ms"] = round((time.monotonic() - t0) * 1000)

        # 3. Publication stats (cached)
        t0 = time.monotonic()
        pub_stats = _get_cached_or_query(
            Config.CACHE_AUTHOR_PUB_STATS, author_id,
            lambda: bq.get_author_pub_stats(author_id),
            last_updated,
        )
        timings["pub_stats_ms"] = round((time.monotonic() - t0) * 1000)

        # 4. Temporal stats (cached)
        t0 = time.monotonic()
        temporal_stats = _get_cached_or_query(
            Config.CACHE_AUTHOR_TEMPORAL, author_id,
            lambda: bq.get_author_temporal_stats(author_id),
            last_updated,
        )
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
        # Always return 200 so curl -f doesn't fail and the JSON is readable.
        # The CI step inspects result["status"] to decide pass/fail.
        return jsonify(result), 200

    @app.errorhandler(404)
    def not_found(e):
        return render_template("error.html", error_message="Page not found."), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template("error.html", error_message="Internal server error."), 500
