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

## Entity Extraction for RAG

The pipeline automatically extracts entities (artists, people, organizations, works, locations) from the annotated content in the same LLM call that generates annotations. Entities are only extracted from text that appears in the annotations (not from content that was filtered out). Entities are written to JSONL files in `src/CrawlToW3C/results/` for use in RAG (Retrieval Augmented Generation) systems.

### Output Format

Entities are saved as `{warc_filename}_entities.jsonl` with one JSON object per line:

```json
{
  "entity": {
    "name": "Pablo Picasso",
    "type": "artist"
  },
  "source": {
    "url": "https://example.com/artist-bio",
    "warc_filename": "crawl-001.warc.gz",
    "warc_date": "2026-01-02T10:30:00Z",
    "warc_record_id": "urn:uuid:...",
    "extracted_at": "2026-01-02T11:00:00Z"
  }
}
```

Entity types: `artist`, `person`, `organization`, `work`, `location`, `other`

The JSONL files are ready for processing by a separate reducer/aggregator tool for RAG indexing.
