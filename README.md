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

## 3. Build & Run

To build and run the full pipeline (crawl and annotation) in one step:

```bash
docker-compose up --build
```

## Output

Final results are written to:

```
src/CrawlToW3C/results/results_collection.json
```
