FROM python:3.8

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt


ENV PORT 8080

EXPOSE 8080

WORKDIR /app

# Command to run on container start
CMD ["python", "main.py"]

