# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libopencv-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create folders
RUN mkdir -p data/bs_data
RUN mkdir img

# Copy the rest of the application code
COPY crawler-twse-bsreport-new.py /app/pyfiles/crawler-twse-bsreport.py
COPY twse_cnn_model.hdf5 /app/data/twse_cnn_model.hdf5

# Define the entrypoint so runtime arguments are passed to the crawler
ENTRYPOINT ["python", "pyfiles/crawler-twse-bsreport.py"]
CMD []
