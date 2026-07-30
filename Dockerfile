FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Karachi \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    HEADLESS=true \
    EVIDENCE_DIR=/tmp/contra-evidence

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        curl \
        ca-certificates \
        tzdata \
        fonts-liberation \
        fonts-noto-core \
        libnss3 \
        libgbm1 \
        libgtk-3-0 \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && echo "Installed browser versions:" \
    && (chromium --version || true) \
    && (chromedriver --version || true) \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN sed -i 's/\r$//' /app/start.sh \
    && chmod +x /app/start.sh \
    && mkdir -p /tmp/contra-evidence

EXPOSE 8080

CMD ["/app/start.sh"]
