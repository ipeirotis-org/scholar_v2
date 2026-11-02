FROM python:3.12-slim

WORKDIR /app

COPY gs_crawler/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY gs_crawler/ /app/gs_crawler/
COPY bigquery/gs_statistics/ /app/bigquery/gs_statistics/

CMD ["python", "-m", "gs_crawler.main"]
