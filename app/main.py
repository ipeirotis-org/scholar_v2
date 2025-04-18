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
import logging
import datetime
import pandas as pd

from shared.config import Config
from scholar import get_similar_authors
from data_analysis import (
    get_author_stats,
    download_all_authors_stats,
    get_publication_stats,
)
from visualization import generate_percentile_rank_plot, generate_pip_plot, generate_pub_citation_plot
from queue_handler import put_author_in_queue, pending_tasks, number_of_tasks_in_queue
from refresh import refresh_authors


from shared.services.storage_service import StorageService


logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.config.from_object(Config)

storage_service = StorageService()


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")



@app.route('/data')
def data():
    return render_template('data.html')

@app.route("/download_all_authors_stats")
def download_all_authors_stats_route():
    # Construct the URL to the file in the GCS bucket
    destination_blob_name = "all_authors_stats.csv"
    file_url = f"https://storage.googleapis.com/{Config.BUCKET_NAME}/{destination_blob_name}"

    if not storage_service.file_updated_within_24_hours(destination_blob_name):
        df = download_all_authors_stats()
        storage_service.upload_csv_to_gcs(df, destination_blob_name)

    # Use this function to get a signed URL and redirect the user to it
    # file_url = storage_service.generate_signed_url(destination_blob_name)
    # Redirect the user to the file URL for download
    return redirect(file_url)



@app.route('/api')
def api():
    return render_template('api.html')

@app.route('/help')
def help():
    return render_template('help.html')


@app.route("/get_similar_authors")
def get_similar_authors_route():
    author_name = request.args.get("author_name")
    authors = get_similar_authors(author_name)
    return jsonify(authors)







@app.route("/api/refresh_authors")
def refresh_authors_route():
    scholar_ids_arg = request.args.get("scholar_ids")
    try:
        scholar_ids = scholar_ids_arg.split(",")
    except Exception as e:
        scholar_ids = None

    num_authors = request.args.get("num_authors")
    try:
        num_authors = int(num_authors)
    except Exception as e:
        num_authors = 1

    if scholar_ids:
        result = refresh_authors(scholar_ids, num_authors=num_authors)
    else:
        result = refresh_authors(num_authors=num_authors)

    if result:
        return jsonify(result)
    else:
        return jsonify({"message": "An error occurred"}), 503


@app.route("/results", methods=["GET"])
def results():
    author_id = request.args.get("author_id", "")

    if not author_id:
        flash("Google Scholar ID is required.")
        return redirect(url_for("index"))

    # Check if there are any tasks about the author in the queue
    if pending_tasks(author_id):
        queue_tasks = number_of_tasks_in_queue()
        return render_template("redirect.html", author_id=author_id, queue_tasks=queue_tasks)

    author = get_author_stats(author_id)

    # If there is no author, put the author in the queue and render redirect.html
    if not author:
        put_author_in_queue(author_id)
        queue_tasks = number_of_tasks_in_queue()
        return render_template("redirect.html", author_id=author_id, queue_tasks=queue_tasks)

    df = pd.DataFrame(author["publications"])
    author_name = author["name"]
    current_year = datetime.datetime.now().year
    df["age"] = current_year - df["pub_year"] + 1
    df["num_citations_percentile"] = 100 * df["num_citations_percentile"]
    df["num_papers_percentile"] = 100 * df["num_papers_percentile"]

    plot1 = generate_percentile_rank_plot(df, author_name)
    plot2 = generate_pip_plot(df, author_name)

    return render_template("results.html", author=author, plot1=plot1, plot2=plot2)


@app.route("/download/<author_id>")
def download_results(author_id):
    author = get_author_stats(author_id)

    # Check if there is data to download
    if len(author["publications"]) == 0:
        flash("No publications found to download.")
        return redirect(url_for("index"))

    downloads_dir = os.path.join(app.root_path, "downloads")
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)  # Create the downloads directory if it doesn't exist

    file_path = os.path.join(downloads_dir, f"{author_id}_results.csv")

    pd.DataFrame(author["publications"]).to_csv(file_path, index=False)

    return send_file(file_path, as_attachment=True, download_name=f"{author_id}_results.csv")


@app.route("/publication/<author_id>/<pub_id>")
def get_publication_details(author_id, pub_id):
    pub_stats = get_publication_stats(author_id, pub_id)
    citations_plot = generate_pub_citation_plot(pd.DataFrame(pub_stats["stats"]))
    if pub_stats:
        return render_template(
            "publication_details.html",
            pub=pub_stats,
            citations_plot=citations_plot,
        )
    else:
        return render_template("error.html", error_message="Publication not found.")


@app.route("/error")
def error():
    return render_template("error.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
