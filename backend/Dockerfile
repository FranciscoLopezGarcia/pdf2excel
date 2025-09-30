
FROM python:3.11-slim

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-spa \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements first for better cache
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Ensure JSON config files in extractors are present (redundant if COPY . . is used, but explicit for clarity)
COPY extractors/*.json extractors/

# Expose backend port
EXPOSE 5000

# Default command
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app", "--workers=3", "--timeout=1800"]

