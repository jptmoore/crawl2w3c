# Crawl-to-W3C 

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

## 3. Build

```bash
docker-compose build
```

## 4. Run

```bash
docker run --rm \
  -v "$PWD/crawls:/app/crawls" \
  -v "$PWD/src:/app/src" \
  crawl-to-w3c
```

## Output

Final results are written to:

```
src/CrawlToW3C/results/results.json
```
