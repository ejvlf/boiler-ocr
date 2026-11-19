FROM python:3.13-slim

# tesseract for OCR; gcc + libmariadb-dev because the mariadb connector compiles on install
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Your custom 'lets' language data (see note below)
COPY lets /usr/share/tesseract-ocr/5/tessdata/lets.traineddata

COPY . .

EXPOSE 8000

# 1 worker + 1 thread: previous_record/stop_recording are in-process globals
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "1", "--timeout", "60", "server:app"]