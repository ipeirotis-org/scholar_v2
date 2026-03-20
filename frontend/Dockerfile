FROM python:3.12-slim

WORKDIR /app

COPY frontend/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# Build matplotlib font cache
RUN MPLBACKEND=Agg python -c "import matplotlib.pyplot"

# Copy all component packages (frontend imports author_search)
COPY frontend/ /app/frontend/
COPY author_search/ /app/author_search/
COPY refresh/ /app/refresh/

EXPOSE 8080
ENV PORT=8080

CMD ["python", "-m", "frontend.main"]
