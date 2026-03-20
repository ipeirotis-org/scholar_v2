FROM python:3.12-slim

WORKDIR /app

COPY v3/frontend/requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Build matplotlib font cache
RUN MPLBACKEND=Agg python -c "import matplotlib.pyplot"

# Copy v3 frontend code
COPY v3/ /app/v3/

EXPOSE 8080
ENV PORT=8080

CMD ["python", "-m", "v3.frontend.main"]
