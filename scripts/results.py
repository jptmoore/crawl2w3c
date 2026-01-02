from CrawlToW3C.process_warc import get_warc_file_paths, iter_html_responses
from CrawlToW3C.html_preprocess import process_html
from CrawlToW3C.url_filter import should_archive
from CrawlToW3C.llms.openai_wrapper import get_client, generate_response
from CrawlToW3C.llms.load_system_prompt import load_system_prompt
from CrawlToW3C.llms.token_count import count_tokens_openai
from CrawlToW3C.entity_writer import write_entities_to_jsonl

import json
import time
import pandas as pd
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# config
TOKEN_BUDGET = 30000
DELAY = 60
RESULTS_DIR = Path("src/CrawlToW3C/results")
CHECKPOINT_JSONL = RESULTS_DIR / "analysis.jsonl"
FINAL_PARQUET = RESULTS_DIR / "analysis.parquet"
STATE_FILE = RESULTS_DIR / "state.json"


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"token_count": 0}


def save_state(state):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def append_checkpoint(record):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_processed_urls():
    processed = set()
    if CHECKPOINT_JSONL.exists():
        with open(CHECKPOINT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if "url" in obj:
                        processed.add(obj["url"])
                except Exception:
                    continue
    return processed


def finalise_parquet():
    if CHECKPOINT_JSONL.exists():
        df = pd.read_json(CHECKPOINT_JSONL, lines=True)
        df.to_parquet(FINAL_PARQUET, index=False)


def main():
    llm = get_client()
    file_paths = get_warc_file_paths()
    system_prompt_gen = load_system_prompt("src/CrawlToW3C/llms/system_prompts.yml", "gpt5_generation")
    system_prompt_filter = load_system_prompt("src/CrawlToW3C/llms/system_prompts.yml", "gpt5_url_selection")
    sys_prompt_tokens = count_tokens_openai(system_prompt_gen)

    state = load_state()
    token_count = int(state.get("token_count", 0))
    processed_urls = read_processed_urls()
    entities_extracted_count = 0

    for url, html, warc_metadata in iter_html_responses(file_paths):
        if url in processed_urls:
            continue

        generated_annotation = None
        processed_html = None
        llm_decision = None

        heuristic_decision = should_archive(str(url))

        if heuristic_decision is True:
            sel = json.loads(generate_response(llm=llm, system_prompt=system_prompt_filter, user_prompt=str(url)))
            llm_decision = sel.get("decision")

            if llm_decision == "archive":
                processed_html = process_html(str(html))
                processed_html = "".join((f"{str(url)}\n\n", processed_html))
                prompt_tokens = sys_prompt_tokens + count_tokens_openai(processed_html)

                if token_count + prompt_tokens > TOKEN_BUDGET:
                    time.sleep(DELAY)
                    token_count = 0

                generated_annotation = generate_response(llm=llm, system_prompt=system_prompt_gen, user_prompt=str(processed_html))
                print(generated_annotation)
                
                # Extract entities from the LLM response
                try:
                    llm_response = json.loads(generated_annotation)
                    extracted_entities = llm_response.get("entities", [])
                    
                    if extracted_entities:
                        write_entities_to_jsonl(
                            entities=extracted_entities,
                            url=url,
                            warc_metadata=warc_metadata,
                            output_dir="src/CrawlToW3C/results"
                        )
                        entities_extracted_count += len(extracted_entities)
                        print(f"Extracted {len(extracted_entities)} entities from {url}")
                except Exception as e:
                    print(f"Warning: Could not extract entities: {e}")

                completion_tokens = count_tokens_openai(generated_annotation) if generated_annotation else 0
                token_count += prompt_tokens + completion_tokens

                if token_count > TOKEN_BUDGET:
                    time.sleep(DELAY)
                    token_count = 0

        record = {
            "url": url,
            "html": html,
            "heuristic_decision": heuristic_decision,
            "llm_decision": llm_decision,
            "processed_html": processed_html,
            "generated_annotation": generated_annotation
        }

        append_checkpoint(record)
        save_state({"token_count": token_count})

    print(f"\nTotal entities extracted: {entities_extracted_count}")
    finalise_parquet()

if __name__ == "__main__":
    main()