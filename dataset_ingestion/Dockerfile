FROM python:3.12-slim

WORKDIR /app

COPY dataset_ingestion/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY dataset_ingestion/ /app/dataset_ingestion/

CMD ["python", "-m", "dataset_ingestion.main"]
