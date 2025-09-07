# backend/Dockerfile
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ensure uvicorn is installed
RUN pip install --no-cache-dir "uvicorn[standard]"

# Copy all backend files
COPY . .

# Expose port 80 (matches fly.toml)
EXPOSE 80

# Run FastAPI with Uvicorn on port 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
