# Use the official Python 3.8 image.
FROM python:3.12‑slim

# Set the working directory to /app
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Import matplotlib the first time to build the font cache.
# Note: This is required only if you are using matplotlib in your project.
RUN MPLBACKEND=Agg python -c "import matplotlib.pyplot" 

# Copy the shared directory into the container at /app/shared
# This ensures that your Docker image includes the shared folder and its contents.
COPY shared/ /app/shared/

# Copy the contents of the app directory into the container at /app
COPY app/ /app/

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable for the port
ENV PORT 8080

# Command to run the Flask application
CMD ["python", "main.py"]
