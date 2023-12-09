from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from util import (
    get_scholar_data,
    get_author_statistics,
    generate_plot,
)
from matplotlib.ticker import MaxNLocator
import matplotlib.pyplot as plt
from flask_caching import Cache
import os
from flask import jsonify
from datetime import datetime
import json
import logging
import time


logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = "secret-key"

app.config["CACHE_TYPE"] = "simple"
cache = Cache(app)

search_history_keys = []


def diagnose_serialization_issue(data, depth=0, max_depth=5):
    try:
        json.dumps(data)
    except TypeError as e:
        logging.error(f"Serialization error at depth {depth}: {e}")

        if depth < max_depth:
            if isinstance(data, dict):
                for key, value in data.items():
                    logging.error(f"Inspecting key: {key}")
                    diagnose_serialization_issue(
                        value, depth=depth + 1, max_depth=max_depth
                    )
            elif isinstance(data, list):
                for index, item in enumerate(data):
                    logging.error(f"Inspecting index: {index}")
                    diagnose_serialization_issue(
                        item, depth=depth + 1, max_depth=max_depth
                    )
            else:
                logging.error(f"Unhandled data type: {type(data)}")
        else:
            logging.error("Max recursion depth reached.")


def json_serial(obj):
    if isinstance(obj, (datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@app.route("/get_similar_authors")
def get_similar_authors():
    author_name = request.args.get("author_name")
    authors, _, _, error = get_scholar_data(author_name, multiple=True)

    if authors is not None:
        clean_authors = []
        for author in authors:
            logging.info(f"Raw author data: {author}")  # Log raw data here
            clean_author = {
                "name": author.get("name"),
                "affiliation": author.get("affiliation"),
                "email": author.get("email"),
                "citedby": author.get("citedby"),
                "scholar_id": author.get("scholar_id"),
            }
            clean_authors.append(clean_author)

            diagnose_serialization_issue(clean_author)

        # Convert clean_authors to a format that is JSON serializable
        serializable_authors = json.dumps(clean_authors, default=json_serial)

        return jsonify(json.loads(serializable_authors))
    else:
        logging.error(f"No authors data found or an error occurred: {error}")
        return jsonify([])


def cleanup_old_images(directory="static", max_age=3600):
    """Delete files older than max_age seconds."""
    now = time.time()

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        file_age = now - os.path.getmtime(file_path)
        if os.path.isfile(file_path) and file_age > max_age:
            os.remove(file_path)


@app.after_request
def add_header(response):
    response.cache_control.no_store = True
    return response


def filter_data_by_timeframe(data, start_year, end_year):
    return {year: data[year] for year in data if start_year <= year <= end_year}


def generate_comparison_charts(
    data1, data2, name1, name2, timestamp, start_year_comparison, end_year_comparison
):
    cleanup_old_images()  # Clean up old images

    years = list(set(data1.keys()).union(data2.keys()))
    years = [
        year
        for year in years
        if year >= start_year_comparison and year <= end_year_comparison
    ]
    years.sort()

    if not years:
        return

    file_prefix = f"comparison_{name1}_vs_{name2}_{timestamp}"

    # Dot plot for Citations
    plt.figure(figsize=(10, 6))
    plt.scatter(
        years,
        [data1.get(year, {}).get("citations", 0) for year in years],
        label=name1,
        marker="o",
    )
    plt.scatter(
        years,
        [data2.get(year, {}).get("citations", 0) for year in years],
        label=name2,
        marker="x",
    )
    plt.legend()
    plt.title("Yearly Citations Comparison")
    plt.xlabel("Year")
    plt.ylabel("Citations")
    plt.grid(True, which="both")
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig(f"static/{file_prefix}_citations.png")
    plt.close()

    # Dot plot for Publications
    plt.figure(figsize=(10, 6))
    plt.scatter(
        years,
        [data1.get(year, {}).get("publications", 0) for year in years],
        label=name1,
        marker="o",
    )
    plt.scatter(
        years,
        [data2.get(year, {}).get("publications", 0) for year in years],
        label=name2,
        marker="x",
    )
    plt.legend()
    plt.title("Yearly Publications Comparison")
    plt.xlabel("Year")
    plt.ylabel("Publications")
    plt.grid(True, which="both")
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig(f"static/{file_prefix}_publications.png")
    plt.close()

    # Dot plot for Scores
    plt.figure(figsize=(10, 6))
    plt.scatter(
        years,
        [data1.get(year, {}).get("scores", 0) for year in years],
        label=name1,
        marker="o",
    )
    plt.scatter(
        years,
        [data2.get(year, {}).get("scores", 0) for year in years],
        label=name2,
        marker="x",
    )
    plt.legend()
    plt.title("Yearly Scores Comparison")
    plt.xlabel("Year")
    plt.ylabel("Scores")
    plt.grid(True, which="both")
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    plt.tight_layout()
    plt.savefig(f"static/{file_prefix}_scores.png")
    plt.close()


@app.route("/")
def index():
    author_count = request.args.get("author_count", default=1, type=int)
    return render_template("index.html", author_count=author_count)


@app.route("/set_author_count", methods=["POST"])
def set_author_count():
    author_count = request.form.get("author_count", default=1, type=int)
    return redirect(url_for("index", author_count=author_count))


def perform_search(author_name):
    author, query, total_publications = get_author_statistics(author_name)
    has_results = not query.empty
    pip_auc_score = 0
    
    try:
        plot_paths, pip_auc_score = generate_plot(query, author["name"]) if has_results else ([], 0)
    except Exception as e:
        logging.error(f"Error generating plot for {author_name}: {e}")
        flash(f"An error occurred while generating the plot for {author_name}.", "error")
        plot_paths, pip_auc_score = [], 0

    search_data = {
        "author": author,
        "results": query,
        "has_results": has_results,
        "plot_paths": plot_paths,
        "total_publications": total_publications,
        "pip_auc_score": pip_auc_score
    }

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    cache_key = f"{author_name}_{timestamp}"
    cache.set(cache_key, search_data)

    current_keys = cache.get("search_history_keys") or []
    current_keys.append(cache_key)
    cache.set("search_history_keys", current_keys)

    return search_data


@app.route("/results", methods=["GET"])
def results():
    author_name = request.args.get("author_name", "")

    if not author_name:
        flash("Author name is required.")
        return redirect(url_for("index"))

    search_data = perform_search(author_name)

    if search_data['has_results']:
        authors_data = [search_data]
    else:
        authors_data = []

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    return render_template(
        "results.html",
        authors_data=authors_data,
        time_stamp=timestamp,
        author_count=1, 
    )



@app.route("/download/<author_name>")
@cache.cached(timeout=3600)  # cache for 1 hour
def download_results(author_name):
    # Fetch the author's data
    author, query, _ = get_author_statistics(author_name)

    # Check if there is data to download
    if query.empty:
        flash("No results found to download.")
        return redirect(url_for("index"))

    downloads_dir = os.path.join(app.root_path, 'downloads')
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)  # Create the downloads directory if it doesn't exist

    file_path = os.path.join(downloads_dir, f"{author_name}_results.csv")

    query.to_csv(file_path, index=False)

    return send_file(
        file_path, as_attachment=True, download_name=f"{author_name}_results.csv"
    )






def save_to_cache(key, data):
    cache.set(key, data)
    if key not in search_history_keys:
        search_history_keys.append(key)


@app.route("/error")
def error():
    return render_template("error.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
