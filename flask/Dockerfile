# Use a more recent and smaller base
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first (better caching)
COPY requirements.txt .

# Install dependencies with timeout and retries
RUN pip install --no-cache-dir \
    --default-timeout=100 \
    -r requirements.txt

# Copy the rest of the app
COPY . .

EXPOSE 5000

# 🔥 THE UPGRADE: Moving from "Student" to "Architect"
# We use Gunicorn to handle the "Firehose" of IoT data
CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "--access-logfile", "-", "--error-logfile", "-", "flask_app:app"]

