"""Flask application factory with security headers and CSRF protection."""

import datetime
import logging

from flask import Flask

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
