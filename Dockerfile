FROM webrecorder/browsertrix-crawler:latest

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y python3-venv ca-certificates && \
    apt-get update && \
    python3 -m venv /opt/venv && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/venv/bin:$PATH"

# Update certificates and configure pip for SSL
RUN update-ca-certificates
RUN pip install --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt
COPY . .
CMD sh -c "crawl --config /app/crawl-config.yaml && make run-main"