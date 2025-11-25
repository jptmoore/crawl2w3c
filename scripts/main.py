
import os
import json
import time
from datetime import datetime, timezone
from CrawlToW3C.process_warc import get_warc_file_paths, iter_html_responses
from CrawlToW3C.html_preprocess import process_html
from CrawlToW3C.url_filter import should_archive, clear_seen_urls
from CrawlToW3C.llms.openai_wrapper import get_client, generate_response
from CrawlToW3C.llms.load_system_prompt import load_system_prompt
from CrawlToW3C.llms.token_count import count_tokens_openai
from dotenv import load_dotenv
load_dotenv()

# config
TOKEN_BUDGET = 30000
DELAY = 60
OUTPUT_FILE = "src/CrawlToW3C/results/results_collection.json"

def main():
    print("Starting Crawl2W3C pipeline...")
    
    # Clear seen URLs from any previous runs
    clear_seen_urls()
    print("Cleared URL cache")
    
    # Check if the archive directory exists before processing
    archive_dir = "/app/collections/one/archive"
    if not os.path.exists(archive_dir):
        print(f"ERROR: Archive directory '{archive_dir}' does not exist. Did the crawl step succeed?")
        return
    
    print("Initializing LLM client...")
    llm = get_client()
    
    print("Loading WARC files...")
    file_paths = get_warc_file_paths()
    print(f"Found {len(file_paths)} WARC files: {file_paths}")
    
    print("Loading system prompts...")
    system_prompt_gen = load_system_prompt("src/CrawlToW3C/llms/system_prompts.yml", "gpt5_generation")
    sys_prompt_gen_tokens = count_tokens_openai(system_prompt_gen)
    print(f"System prompt loaded ({sys_prompt_gen_tokens} tokens)")

    # Ensure results directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Prepare annotation collection - store AnnotationPages
    annotation_pages = []
    warc_filenames = set()
    token_count = 0

    print("Starting to process URLs from WARC files...")
    url_count = 0
    for url, html, warc_metadata in iter_html_responses(file_paths):
        url_count += 1
        
        generated_annotation = None
        processed_html = None

        # Use heuristic filter for URL-based filtering
        heuristic_decision = should_archive(str(url))

        if heuristic_decision is True:
            print(f"Processing URL {url_count}: {url}")
            original_html = str(html)
            processed_html = process_html(original_html)
            processed_html = f"{str(url)}\n\n{processed_html}"
            processed_html_tokens = count_tokens_openai(processed_html)

            # Generate annotations - LLM will decide what's worth annotating
            gen_prompt_tokens = sys_prompt_gen_tokens + processed_html_tokens

            if token_count + gen_prompt_tokens > TOKEN_BUDGET:
                print(f"Token budget reached, sleeping for {DELAY} seconds...")
                time.sleep(DELAY)
                token_count = 0

            print(f"Generating annotations for {url}...")
            generated_annotation = generate_response(
                llm=llm,
                system_prompt=system_prompt_gen,
                user_prompt=processed_html
            )

            generated_annotation_page = json.loads(generated_annotation)

            completion_tokens = count_tokens_openai(generated_annotation) if generated_annotation else 0
            token_count += gen_prompt_tokens + completion_tokens

            if token_count > TOKEN_BUDGET:
                time.sleep(DELAY)
                token_count = 0

            # Collect WARC filename for collection label
            warc_filenames.add(warc_metadata.get("warc_filename"))
            
            # Add metadata to the AnnotationPage
            collection_id = "urn:uuid:collection-001"
            page_metadata = {
                "partOf": collection_id,
                "created": warc_metadata.get("warc_date"),
                "generator": {
                    "id": "urn:crawl2w3c:v1",
                    "type": "Software",
                    "name": "Crawl2W3C",
                    "homepage": "https://github.com/jptmoore/crawl2w3c"
                },
                "generated": warc_metadata.get("warc_date"),
                "source": {
                    "warc_record_id": warc_metadata.get("warc_record_id"),
                    "warc_ip_address": warc_metadata.get("warc_ip_address"),
                    "warc_payload_digest": warc_metadata.get("warc_payload_digest"),
                    "http_server": warc_metadata.get("http_server"),
                    "http_last_modified": warc_metadata.get("http_last_modified")
                }
            }

            # Add the AnnotationPage to the collection (only if it has items)
            if isinstance(generated_annotation_page, dict) and generated_annotation_page.get("type") == "AnnotationPage":
                items = generated_annotation_page.get("items", [])
                # Only add if there are actual annotations
                if items:
                    print(f"✓ Generated {len(items)} annotations for {url}")
                    # Add an ID and metadata to the AnnotationPage if it doesn't have them
                    if "id" not in generated_annotation_page:
                        generated_annotation_page["id"] = f"urn:uuid:page-{len(annotation_pages)+1}"
                    
                    # Reorder to put metadata after type and before items
                    generated_annotation_page.pop("items", [])
                    generated_annotation_page.update(page_metadata)
                    generated_annotation_page["items"] = items
                    annotation_pages.append(generated_annotation_page)
                else:
                    print(f"Skipping {url} (no annotations generated)")
            elif isinstance(generated_annotation_page, dict) and "items" in generated_annotation_page:
                items = generated_annotation_page["items"]
                # Only add if there are actual annotations
                if items:
                    print(f"✓ Generated {len(items)} annotations for {url}")
                    # Convert to proper AnnotationPage if missing type
                    page = {
                        "@context": "http://www.w3.org/ns/anno.jsonld",
                        "type": "AnnotationPage",
                        "id": f"urn:uuid:page-{len(annotation_pages)+1}",
                        **page_metadata,
                        "items": items
                    }
                    annotation_pages.append(page)
                else:
                    print(f"Skipping {url} (no annotations generated)")
            elif isinstance(generated_annotation_page, list) and generated_annotation_page:
                # Only add if there are actual annotations
                print(f"✓ Generated {len(generated_annotation_page)} annotations for {url}")
                # Wrap array of annotations in AnnotationPage
                page = {
                    "@context": "http://www.w3.org/ns/anno.jsonld",
                    "type": "AnnotationPage",
                    "id": f"urn:uuid:page-{len(annotation_pages)+1}",
                    **page_metadata,
                    "items": generated_annotation_page
                }
                annotation_pages.append(page)
            else:
                print(f"Skipping {url} (no annotations generated)")
        else:
            print(f"Skipping URL (heuristic filter): {url}")

    print(f"Finished processing {url_count} URLs")
    print(f"Generated {len(annotation_pages)} annotation pages")

    # Create label with WARC filename(s)
    warc_files_str = ", ".join(sorted(warc_filenames)) if warc_filenames else "Unknown WARC"
    collection_label = f"Crawl2W3C Annotation Collection - {warc_files_str}"
    
    # Write W3C Web Annotation Collection containing AnnotationPages with rich metadata
    collection_id = "urn:uuid:collection-001"
    collection = {
        "@context": "http://www.w3.org/ns/anno.jsonld",
        "id": collection_id,
        "type": "AnnotationCollection",
        "label": collection_label,
        "creator": {
            "id": "urn:crawl2w3c:v1",
            "type": "Software",
            "name": "Crawl2W3C",
            "homepage": "https://github.com/jptmoore/crawl2w3c"
        },
        "created": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "generator": {
            "id": "urn:openai:gpt-5",
            "type": "Software",
            "name": "OpenAI GPT-5",
            "homepage": "https://openai.com"
        },
        "generated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        "items": annotation_pages
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(collection, f, ensure_ascii=False, indent=2)

    # Upload to Miiify server as part of pipeline
    try:
        from CrawlToW3C.miiify_client import upload_collection_to_miiify, MiiifyClient
        
        # Give Miiify server a moment to be ready
        time.sleep(5)
        
        client = MiiifyClient(base_url="http://miiify:10000")
        results = upload_collection_to_miiify(collection, client)
        
        if results['container_created'] and not results['errors']:
            msg = f"✓ Uploaded {results['annotations_uploaded']} annotations to container: {results.get('container_slug', 'unknown')}"
            if results.get('annotations_skipped', 0) > 0:
                msg += f" (skipped {results['annotations_skipped']} duplicates)"
            print(msg)
        else:
            print(f"Upload errors: {results['errors']}")
            
    except ImportError:
        print("Miiify client not available - skipping upload")
    except Exception as e:
        print(f"Error uploading to Miiify: {e}")


if __name__ == "__main__":
    main()
