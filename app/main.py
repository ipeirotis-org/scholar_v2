from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    jsonify,
)


import os
import json
import logging

import pandas as pd

import numpy as np


from sklearn.metrics import auc

from scholar import get_author, get_similar_authors, get_publication
from data_analysis import get_author_stats
from visualization import generate_plot


logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = "secret-key"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/get_similar_authors")
def get_similar_authors_route():
    author_name = request.args.get("author_name")
    authors = get_similar_authors(author_name)
    return jsonify(authors)


@app.route("/api/author/<author_id>")
def get_author_route(author_id):
    author = get_author(author_id)
    return jsonify(author)

@app.route("/api/author_stats/<author_id>")
def get_author_stats_route(author_id):
    author = get_author_stats(author_id)
    return jsonify(author)


@app.route("/api/author/<author_id>/publication/<pub_id>")
def get_publication_route(author_id, pub_id):
    pub = get_publication(author_id, pub_id)
    return jsonify(pub)


@app.route("/results", methods=["GET"])
def results():
    author_id = request.args.get("author_id", "")

    if not author_id:
        flash("Google Scholar ID is required.")
        return redirect(url_for("index"))

    author = get_author_stats(author_id)
    plot_paths = generate_plot(pd.DataFrame(author['publications']), author["name"])
    author["plot_paths"] = plot_paths
    return render_template("results.html", author=author)


@app.route("/download/<author_id>")
def download_results(author_id):

    author = get_author_stats(author_id)

    # Check if there is data to download
    if len(author['publications'])==0 :
        flash("No publications found to download.")
        return redirect(url_for("index"))

    downloads_dir = os.path.join(app.root_path, "downloads")
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)  # Create the downloads directory if it doesn't exist

    file_path = os.path.join(downloads_dir, f"{author_id}_results.csv")

    pd.DataFrame(author['publications']).to_csv(file_path, index=False)

    return send_file(
        file_path, as_attachment=True, download_name=f"{author_id}_results.csv"
    )


@app.route("/error")
def error():
    return render_template("error.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
