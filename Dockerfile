FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps for Pillow/reportlab if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libjpeg62-turbo-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

COPY . /app

ENV SESSION_COOKIE_SECURE=1 \
    PREFERRED_URL_SCHEME=https

EXPOSE 5000

CMD ["gunicorn", "-w", "3", "-k", "gthread", "-b", "0.0.0.0:5000", "wsgi:application"]

