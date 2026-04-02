FROM python:3.12-slim

WORKDIR /app

COPY frontend/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Copy all component packages (frontend imports author_search and shared)
COPY frontend/ /app/frontend/
COPY author_search/ /app/author_search/
COPY shared/ /app/shared/

EXPOSE 8080
ENV PORT=8080

CMD ["python", "-m", "frontend.main"]
