# syntax=docker/dockerfile:1

FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    python3-dev \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_VERSION=3.9.1

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Copy Poetry configuration files
COPY poetry.lock pyproject.toml ./

# Install dependencies (runtime only)
RUN poetry install --only main

# Copy the entire project
COPY src ./src/

# Set the PYTHONPATH to include the src/ directory
ENV PYTHONPATH=/app/src

# Expose the port that the application will run on
EXPOSE 8000

# Command to run the application
CMD ["poetry", "run", "gunicorn", "src.main:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]
