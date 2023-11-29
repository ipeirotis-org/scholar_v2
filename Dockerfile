FROM python:3.8

WORKDIR /app

COPY Desktop/myapp_nostartendyear/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY Desktop/myapp_nostartendyear/ .

ENV PORT 8080

EXPOSE 8080

# Command to run on container start
CMD ["python", "main.py"]

