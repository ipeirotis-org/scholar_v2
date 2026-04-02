FROM python:3.12-slim

WORKDIR /app

COPY cache_layer/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY cache_layer/ /app/cache_layer/
COPY shared/ /app/shared/

EXPOSE 8081
ENV PORT=8081

CMD ["python", "-m", "cache_layer.main"]
