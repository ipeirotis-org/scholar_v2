FROM python:3.8

WORKDIR /app

COPY Desktop/myapp_nostartendyear/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY Desktop/myapp_nostartendyear/ .

EXPOSE 5000

# Command to run on container start
CMD ["python", "main.py"]

