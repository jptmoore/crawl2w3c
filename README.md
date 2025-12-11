# Crawl-to-W3C 

A pipeline that crawls web content and generates W3C Web Annotations using LLMs, with automatic upload to a Miiify annotation server.

## 1. Environment

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=sk-your_api_key
```

### Optional Configuration

You can configure annotation ID generation by setting these environment variables:

```env
MIIIFY_HOST=localhost          # Default: localhost - Hostname for annotation IDs
MIIIFY_ID_PROTO=http          # Default: http - Protocol for annotation IDs (http/https)
MIIIFY_PORT=10000             # Default: 10000 - Port for Miiify server
```

These configure how annotation IDs are generated, for example:
- `http://localhost:10000/annotations/...`

## 2. Configure seeds

Edit `crawl-config.yaml`:

```yaml
seeds:
  - https://example.com
```

Or use a custom config file from another location:

```bash
CRAWL_CONFIG=./examples/my-config.yaml docker-compose up --build
```

## 3. Run the Pipeline

Basic usage:

```bash
docker-compose up --build
```

With custom config:

```bash
CRAWL_CONFIG=./examples/crawl-config.yaml docker-compose up --build
```

With custom annotation ID configuration:

```bash
MIIIFY_HOST=example.com MIIIFY_ID_PROTO=https CRAWL_CONFIG=./examples/crawl-config.yaml docker-compose up --build
```


