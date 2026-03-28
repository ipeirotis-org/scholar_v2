"""Flask application factory with security headers and CSRF protection."""

import datetime
import logging

from flask import Flask, redirect, request

from frontend.config import Config

logger = logging.getLogger(__name__)


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

    @app.before_request
    def redirect_old_domain():
        """Redirect scholar-analytics.org traffic to pip-score.org."""
        host = request.host.lower()
        if "scholar-analytics.org" in host:
            new_url = request.url.replace(request.host, "www.pip-score.org", 1)
            return redirect(new_url, code=301)

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

    return app
