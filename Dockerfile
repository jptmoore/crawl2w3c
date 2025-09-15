FROM webrecorder/browsertrix-crawler:latest

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y python3-venv && \
    python3 -m venv /opt/venv && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD sh -c "crawl --config /app/crawl-config.yaml && make run-main"