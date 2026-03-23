"""Flask application factory with security headers and CSRF protection."""

import datetime
import logging
import os
import threading

from flask import Flask

from frontend.config import Config

logger = logging.getLogger(__name__)


def _start_region_health_scorer():
    """Start a daemon thread that periodically updates region health scores."""
    from region_health.config import SCORER_INTERVAL_SECONDS

    def _scorer_loop():
        import time
        from region_health.scorer import update_scores
        while True:
            try:
                update_scores()
            except Exception:
                logger.exception("Region health scorer iteration failed")
            time.sleep(SCORER_INTERVAL_SECONDS)

    t = threading.Thread(target=_scorer_loop, daemon=True, name="region-health-scorer")
    t.start()
    logger.info("Started region health scorer background thread")


def create_app(config=None):
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    app.config.from_object(Config)
    if config:
        app.config.update(config)

    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
        return response

    @app.context_processor
    def inject_globals():
        return {
            "current_year": datetime.datetime.now().year,
            "ga_tracking_id": Config.GA_TRACKING_ID,
        }

    from frontend.routes import register_routes

    register_routes(app)

    # Start the background scorer in production (not during testing)
    if not app.config.get("TESTING") and os.environ.get("DISABLE_HEALTH_SCORER") != "1":
        _start_region_health_scorer()

    return app
