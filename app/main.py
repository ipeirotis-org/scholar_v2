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
from data_analysis import get_author_statistics_by_id
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
    pub = get_author(author_id)
    return jsonify(pub)


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

    author, publications, total_publications, pip_auc_score, pip_auc_percentile, total_publications_percentile, first_year_active = get_author_statistics_by_id(author_id)


    if publications.empty:
        flash("Google Scholar ID has no data.")
        return redirect(url_for("index"))

    try:
        plot_paths = generate_plot(publications, author["name"])
    except Exception as e:
        logging.error(f"Error generating plot for {author_id}: {e}")
        flash(f"An error occurred while generating the plot for {author_id}.", "error")
        plot_paths = []

    author = {
        "author": author,
        "publications": publications,
        "plot_paths": plot_paths,
        "total_publications": total_publications,
        "pip_auc_score": pip_auc_score,
        "pip_auc_percentile": pip_auc_percentile,
        "total_publications_percentile": total_publications_percentile,
        "first_year_active": first_year_active
    }
    
    return render_template('results.html', author=author)



@app.route("/download/<author_id>")
def download_results(author_id):
    author_info, publications, total_publications, pip_auc, total_publications_percentile, first_year_active = get_author_statistics_by_id(author_id)


    # Check if there is data to download
    if publications.empty:
        flash("No publications found to download.")
        return redirect(url_for("index"))

    downloads_dir = os.path.join(app.root_path, "downloads")
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)  # Create the downloads directory if it doesn't exist

    file_path = os.path.join(downloads_dir, f"{author_id}_results.csv")

    publications.to_csv(file_path, index=False)

    return send_file(
        file_path, as_attachment=True, download_name=f"{author_id}_results.csv"
    )


@app.route("/error")
def error():
    return render_template("error.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
