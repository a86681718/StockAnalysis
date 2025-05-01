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
COPY pyfiles/Crawler_TWSEBuySellReport.py /app/pyfiles/Crawler_TWSEBuySellReport.py
COPY pyfiles/twse_cnn_model.hdf5 /app/data/twse_cnn_model.hdf5

# Define the command to run the application
CMD ["python", "pyfiles/Crawler_TWSEBuySellReport.py"]