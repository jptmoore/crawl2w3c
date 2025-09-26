
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
    # Clear seen URLs from any previous runs
    clear_seen_urls()
    
    # Check if the archive directory exists before processing
    archive_dir = "/app/collections/one/archive"
    if not os.path.exists(archive_dir):
        print(f"ERROR: Archive directory '{archive_dir}' does not exist. Did the crawl step succeed?")
        return
    
    llm = get_client()
    file_paths = get_warc_file_paths()
    system_prompt_gen = load_system_prompt("src/CrawlToW3C/llms/system_prompts.yml", "gpt5_generation")
    system_prompt_filter = load_system_prompt("src/CrawlToW3C/llms/system_prompts.yml", "gpt5_url_selection")
    sys_prompt_tokens = count_tokens_openai(system_prompt_gen)

    # Ensure results directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Prepare annotation collection - store AnnotationPages
    annotation_pages = []
    warc_filenames = set()
    token_count = 0

    print(f"Processing {len(file_paths)} WARC files...")
    url_count = 0
    for url, html, warc_metadata in iter_html_responses(file_paths):
        url_count += 1
        print(f"Processing URL {url_count}: {url}")
        
        generated_annotation = None
        processed_html = None
        llm_decision = None

        heuristic_decision = should_archive(str(url))
        print(f"Heuristic decision for {url}: {heuristic_decision}")

        original_html = str(html)

        processed_html = process_html(original_html)
        processed_html = f"{str(url)}\n\n{processed_html}"
        processed_html_tokens = count_tokens_openai(processed_html)

        if heuristic_decision is True:
            print(f"Getting LLM decision for: {url}")
            sel = generate_response(
                llm=llm,
                system_prompt=system_prompt_filter,
                user_prompt=str(url)
            )
            print(f"LLM filter response: {sel}")

            llm_decision = json.loads(sel)
            llm_decision = llm_decision["decision"]
            print(f"LLM decision: {llm_decision}")

            if llm_decision == "archive":
                print(f"Archiving URL: {url}")
                prompt_tokens = sys_prompt_tokens + processed_html_tokens

                if token_count + prompt_tokens > TOKEN_BUDGET:
                    time.sleep(DELAY)
                    token_count = 0

                print("Generating annotations with LLM...")
                generated_annotation = generate_response(
                    llm=llm,
                    system_prompt=system_prompt_gen,
                    user_prompt=processed_html
                )
                print(f"Generated annotation response (length: {len(generated_annotation) if generated_annotation else 0})")

                generated_annotation_page = json.loads(generated_annotation)
                print(f"Parsed annotation page with {len(generated_annotation_page.get('items', []))} items")

                completion_tokens = count_tokens_openai(generated_annotation) if generated_annotation else 0
                token_count += prompt_tokens + completion_tokens

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

                # Add the AnnotationPage to the collection
                if isinstance(generated_annotation_page, dict) and generated_annotation_page.get("type") == "AnnotationPage":
                    # Add an ID and metadata to the AnnotationPage if it doesn't have them
                    if "id" not in generated_annotation_page:
                        generated_annotation_page["id"] = f"urn:uuid:page-{len(annotation_pages)+1}"
                    
                    # Reorder to put metadata after type and before items
                    items = generated_annotation_page.pop("items", [])
                    generated_annotation_page.update(page_metadata)
                    generated_annotation_page["items"] = items
                    annotation_pages.append(generated_annotation_page)
                elif isinstance(generated_annotation_page, dict) and "items" in generated_annotation_page:
                    # Convert to proper AnnotationPage if missing type
                    page = {
                        "@context": "http://www.w3.org/ns/anno.jsonld",
                        "type": "AnnotationPage",
                        "id": f"urn:uuid:page-{len(annotation_pages)+1}",
                        **page_metadata,
                        "items": generated_annotation_page["items"]
                    }
                    annotation_pages.append(page)
                elif isinstance(generated_annotation_page, list):
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
            print(f"Skipping URL (heuristic: {heuristic_decision}): {url}")

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
    print("Starting Miiify upload...")
    try:
        from CrawlToW3C.miiify_client import upload_collection_to_miiify, MiiifyClient
        
        # Give Miiify server a moment to be ready
        print("Waiting 5 seconds for Miiify server...")
        time.sleep(5)
        
        # Upload directly to Miiify server
        print("Uploading collection to Miiify server...")
        client = MiiifyClient(base_url="http://miiify:10000")
        results = upload_collection_to_miiify(collection, client)
        
        if results['container_created'] and not results['errors']:
            print(f"✅ Successfully uploaded {results['annotations_uploaded']} annotations to Miiify server")
        else:
            print(f"⚠️ Upload completed with errors: {results['errors']}")
            
    except ImportError:
        print("❌ Miiify client not available - skipping upload")
    except Exception as e:
        print(f"❌ Error uploading to Miiify: {e}")


if __name__ == "__main__":
    main()
