
import os
import json
import time
from CrawlToW3C.process_warc import get_warc_file_paths, iter_html_responses
from CrawlToW3C.html_preprocess import process_html
from CrawlToW3C.url_filter import should_archive
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


    for url, html in iter_html_responses(file_paths):
        generated_annotation = None
        processed_html = None
        llm_decision = None
        token_count = 0

        heuristic_decision = should_archive(str(url))

        original_html = str(html)

        processed_html = process_html(original_html)
        processed_html = f"{str(url)}\n\n{processed_html}"
        processed_html_tokens = count_tokens_openai(processed_html)

        if heuristic_decision is True:
            sel = generate_response(
                llm=llm,
                system_prompt=system_prompt_filter,
                user_prompt=str(url)
            )

            llm_decision = json.loads(sel)
            llm_decision = llm_decision["decision"]

            if llm_decision == "archive":
                prompt_tokens = sys_prompt_tokens + processed_html_tokens

                if token_count + prompt_tokens > TOKEN_BUDGET:
                    time.sleep(DELAY)
                    token_count = 0

                generated_annotation = generate_response(
                    llm=llm,
                    system_prompt=system_prompt_gen,
                    user_prompt=processed_html
                )

                generated_annotation_page = json.loads(generated_annotation)

                completion_tokens = count_tokens_openai(generated_annotation) if generated_annotation else 0
                token_count += prompt_tokens + completion_tokens

                if token_count > TOKEN_BUDGET:
                    time.sleep(DELAY)
                    token_count = 0

                # Add the AnnotationPage to the collection
                if isinstance(generated_annotation_page, dict) and generated_annotation_page.get("type") == "AnnotationPage":
                    # Add an ID to the AnnotationPage if it doesn't have one
                    if "id" not in generated_annotation_page:
                        generated_annotation_page["id"] = f"urn:uuid:page-{len(annotation_pages)+1}"
                    annotation_pages.append(generated_annotation_page)
                elif isinstance(generated_annotation_page, dict) and "items" in generated_annotation_page:
                    # Convert to proper AnnotationPage if missing type
                    page = {
                        "@context": "http://www.w3.org/ns/anno.jsonld",
                        "id": f"urn:uuid:page-{len(annotation_pages)+1}",
                        "type": "AnnotationPage",
                        "items": generated_annotation_page["items"]
                    }
                    annotation_pages.append(page)
                elif isinstance(generated_annotation_page, list):
                    # Wrap array of annotations in AnnotationPage
                    page = {
                        "@context": "http://www.w3.org/ns/anno.jsonld",
                        "id": f"urn:uuid:page-{len(annotation_pages)+1}",
                        "type": "AnnotationPage",
                        "items": generated_annotation_page
                    }
                    annotation_pages.append(page)

    # Write W3C Web Annotation Collection containing AnnotationPages
    collection = {
        "@context": "http://www.w3.org/ns/anno.jsonld",
        "id": "urn:uuid:collection-001",
        "type": "AnnotationCollection",
        "label": "Crawl2W3C Annotation Collection",
        "items": annotation_pages
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(collection, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
