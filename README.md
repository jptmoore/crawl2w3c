# Crawl-to-W3C 

A pipeline that crawls web content and generates W3C Web Annotations using LLMs, with automatic upload to a Miiify annotation server.

## 1. Environment

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=sk-your_api_key
```

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

```bash
docker-compose up --build
```

With custom config:

```bash
CRAWL_CONFIG=./examples/crawl-config.yaml docker-compose up --build
```

## Pipeline Steps

1. **Crawl**: Download web content using configured seeds
2. **Process**: Extract and preprocess HTML content  
3. **Annotate**: Generate W3C Web Annotations using LLM
4. **Upload**: Store annotations in Miiify server with persistent IDs
