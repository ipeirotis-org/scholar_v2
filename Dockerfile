# Use the official Python 3.8 image.
FROM python:3.8

# Set the working directory to /app
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Import matplotlib the first time to build the font cache
RUN MPLBACKEND=Agg python -c "import matplotlib.pyplot" 

# Copy the contents of the app directory into the container at /app
COPY app/ /app/

COPY data/ /data/

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable for the port
ENV PORT 8080

# Command to run the Flask application
CMD ["python", "main.py"]
