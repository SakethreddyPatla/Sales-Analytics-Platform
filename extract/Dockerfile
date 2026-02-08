FROM python:3.11-slim

WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy extraction scripts
COPY extract_api_data.py .
COPY generate_customer_data.py .

# Create data directory
RUN mkdir -p /app/data/raw

CMD ["python", "extract_api_data.py"]
