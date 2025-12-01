
import os
import json
import time
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

    # Initialize Miiify client for incremental uploads
    print("Initializing Miiify client...")
    miiify_client = None
    container_slug = None
    annotations_uploaded = 0
    annotations_skipped = 0
    
    try:
        from CrawlToW3C.miiify_client import MiiifyClient, create_container_slug, extract_slug_from_annotation_id
        
        # Give Miiify server a moment to be ready
        time.sleep(5)
        
        # Get optional Host header from environment (should include port for non-standard ports)
        miiify_host = os.getenv('MIIIFY_HOST', 'localhost')
        miiify_port = os.getenv('MIIIFY_PORT', '10000')
        # Build Host header with port for non-standard ports
        host_header = f"{miiify_host}:{miiify_port}"
        print(f"Using Host header: {host_header}")
        miiify_client = MiiifyClient(base_url="http://miiify:10000", host=host_header)
        
        # Create container once at the start
        warc_files_str = None
        if file_paths:
            warc_filename = os.path.basename(file_paths[0])
            warc_files_str = warc_filename
        
        collection_id = "urn:uuid:collection-001"
        container_slug = create_container_slug(collection_id, warc_files_str)
        
        container_metadata = {
            "@context": "http://iiif.io/api/presentation/3/context.json",
            "type": "AnnotationCollection",
            "label": f"Crawl2W3C Annotation Collection - {warc_files_str or 'Unknown WARC'}"
        }
        
        miiify_client.create_container(container_slug, container_metadata)
        print(f"Created Miiify container: {container_slug}")
        
    except ImportError:
        print("Miiify client not available - annotations will be lost")
    except Exception as e:
        print(f"Warning: Could not initialize Miiify client: {e}")
        miiify_client = None

    annotation_pages_count = 0
    token_count = 0

    print("="*60)
    print("Starting to process URLs from WARC files...")
    print("="*60)
    url_count = 0
    for url, html, warc_metadata in iter_html_responses(file_paths):
        url_count += 1
        print(f"\n[{url_count}] Examining URL: {url}")
        
        generated_annotation = None
        processed_html = None

        # Use heuristic filter for URL-based filtering
        heuristic_decision = should_archive(str(url))

        if heuristic_decision is True:
            print(f"  → Accepted by filter, processing content...")
            original_html = str(html)
            processed_html = process_html(original_html)
            processed_html = f"{str(url)}\n\n{processed_html}"
            processed_html_tokens = count_tokens_openai(processed_html)

            # Generate annotations - LLM will decide what's worth annotating
            gen_prompt_tokens = sys_prompt_gen_tokens + processed_html_tokens

            if token_count + gen_prompt_tokens > TOKEN_BUDGET:
                print(f"  ⚠ Token budget reached, sleeping for {DELAY} seconds...")
                time.sleep(DELAY)
                token_count = 0

            print(f"  → Calling LLM to generate annotations...")
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
                    print(f"  ✓ Generated {len(items)} annotations")
                    annotation_pages_count += 1
                    # Add an ID and metadata to the AnnotationPage if it doesn't have them
                    if "id" not in generated_annotation_page:
                        generated_annotation_page["id"] = f"urn:uuid:page-{annotation_pages_count}"
                    
                    # Reorder to put metadata after type and before items
                    generated_annotation_page.pop("items", [])
                    generated_annotation_page.update(page_metadata)
                    generated_annotation_page["items"] = items
                    
                    # Upload to Miiify immediately
                    if miiify_client and container_slug:
                        print(f"  → Uploading {len(items)} annotations to Miiify...")
                        for annotation in items:
                            if 'id' in annotation:
                                try:
                                    annotation_slug = extract_slug_from_annotation_id(annotation['id'])
                                    result = miiify_client.upload_annotation(container_slug, annotation_slug, annotation)
                                    if isinstance(result, dict) and result.get('skipped'):
                                        annotations_skipped += 1
                                    else:
                                        annotations_uploaded += 1
                                except Exception as e:
                                    print(f"    ⚠ Error uploading annotation: {e}")
                else:
                    print(f"  ✗ No annotations generated (content not substantial enough)")
            elif isinstance(generated_annotation_page, dict) and "items" in generated_annotation_page:
                items = generated_annotation_page["items"]
                # Only add if there are actual annotations
                if items:
                    print(f"  ✓ Generated {len(items)} annotations")
                    annotation_pages_count += 1
                    # Convert to proper AnnotationPage if missing type
                    page = {
                        "@context": "http://www.w3.org/ns/anno.jsonld",
                        "type": "AnnotationPage",
                        "id": f"urn:uuid:page-{annotation_pages_count}",
                        **page_metadata,
                        "items": items
                    }
                    
                    # Upload to Miiify immediately
                    if miiify_client and container_slug:
                        print(f"  → Uploading {len(items)} annotations to Miiify...")
                        for annotation in items:
                            if 'id' in annotation:
                                try:
                                    annotation_slug = extract_slug_from_annotation_id(annotation['id'])
                                    result = miiify_client.upload_annotation(container_slug, annotation_slug, annotation)
                                    if isinstance(result, dict) and result.get('skipped'):
                                        annotations_skipped += 1
                                    else:
                                        annotations_uploaded += 1
                                except Exception as e:
                                    print(f"    ⚠ Error uploading annotation: {e}")
                else:
                    print(f"  ✗ No annotations generated (content not substantial enough)")
            elif isinstance(generated_annotation_page, list) and generated_annotation_page:
                # Only add if there are actual annotations
                print(f"  ✓ Generated {len(generated_annotation_page)} annotations")
                annotation_pages_count += 1
                # Wrap array of annotations in AnnotationPage
                page = {
                    "@context": "http://www.w3.org/ns/anno.jsonld",
                    "type": "AnnotationPage",
                    "id": f"urn:uuid:page-{annotation_pages_count}",
                    **page_metadata,
                    "items": generated_annotation_page
                }
                
                # Upload to Miiify immediately
                if miiify_client and container_slug:
                    print(f"  → Uploading {len(generated_annotation_page)} annotations to Miiify...")
                    for annotation in generated_annotation_page:
                        if 'id' in annotation:
                            try:
                                annotation_slug = extract_slug_from_annotation_id(annotation['id'])
                                result = miiify_client.upload_annotation(container_slug, annotation_slug, annotation)
                                if isinstance(result, dict) and result.get('skipped'):
                                    annotations_skipped += 1
                                else:
                                    annotations_uploaded += 1
                            except Exception as e:
                                print(f"    ⚠ Error uploading annotation: {e}")
            else:
                print(f"  ✗ No annotations generated (content not substantial enough)")
        else:
            print(f"  ✗ Rejected by filter (URL pattern/extension)")

    print("="*60)
    print(f"COMPLETED: Processed {url_count} URLs")
    print(f"Generated annotations from {annotation_pages_count} URLs")
    print("="*60)

    # Report Miiify upload results
    if miiify_client and container_slug:
        msg = f"✓ Uploaded {annotations_uploaded} annotations to container: {container_slug}"
        if annotations_skipped > 0:
            msg += f" (skipped {annotations_skipped} duplicates)"
        print(msg)


if __name__ == "__main__":
    main()
