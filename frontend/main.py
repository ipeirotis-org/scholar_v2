"""Entry point for the v3 frontend Flask application."""

from frontend.app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
