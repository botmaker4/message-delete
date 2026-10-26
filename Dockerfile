FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MESSAGES_PATH=/data/package.zip \
    PROGRESS_DB=/data/progress.sqlite3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY main.py ./
COPY discord_cleaner ./discord_cleaner

RUN groupadd --gid 10001 cleaner \
    && useradd --uid 10001 --gid cleaner --no-create-home cleaner \
    && mkdir -p /data \
    && chown cleaner:cleaner /data

USER cleaner

CMD ["python", "main.py"]
