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

COPY lets/lets.traineddata /usr/share/tesseract-ocr/5/tessdata/lets.traineddata
COPY lets/lets.pffmtable /usr/share/tesseract-ocr/5/tessdata/lets.pffmtable
COPY lets/lets.shapetable /usr/share/tesseract-ocr/5/tessdata/lets.shapetable
COPY lets/lets.unicharset /usr/share/tesseract-ocr/5/tessdata/lets.unicharset

COPY . .

EXPOSE 8000

# 1 worker + 1 thread: previous_record/stop_recording are in-process globals
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "1", "--timeout", "60", "server:app"]