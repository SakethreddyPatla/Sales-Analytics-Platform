FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy load scripts
COPY load_to_db.py .
COPY verify_database.py .

# Create data directory
RUN mkdir -p /app/data

