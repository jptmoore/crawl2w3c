"""
Entity Writer

This module provides functionality to write extracted entities to JSONL files
for later processing in RAG systems.
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any


def write_entities_to_jsonl(entities: List[Dict[str, Any]], url: str, 
                            warc_metadata: Dict[str, Any], 
                            output_dir: str = None) -> str:
    """
    Write extracted entities to a JSONL file.
    
    Each entity is written as a single line JSON object with metadata
    about the source URL and WARC information for provenance.
    
    Args:
        entities: List of entity dictionaries with name, type, and context
        url: Source URL where entities were extracted from
        warc_metadata: WARC metadata for provenance tracking
        output_dir: Directory to write JSONL files (defaults to results/)
        
    Returns:
        Path to the written JSONL file
    """
    if output_dir is None:
        # Use the existing results directory
        output_dir = "/app/src/CrawlToW3C/results"
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename based on WARC filename
    warc_filename = warc_metadata.get("warc_filename", "unknown")
    base_name = warc_filename.replace('.warc.gz', '').replace('.warc', '')
    output_file = os.path.join(output_dir, f"{base_name}_entities.jsonl")
    
    # Write entities to JSONL
    with open(output_file, 'a', encoding='utf-8') as f:
        for entity in entities:
            # Clean entity - only keep name and type (remove context if LLM included it)
            clean_entity = {
                "name": entity.get("name"),
                "type": entity.get("type")
            }
            
            # Enrich entity with provenance metadata
            enriched_entity = {
                "entity": clean_entity,
                "source": {
                    "url": url,
                    "warc_filename": warc_metadata.get("warc_filename"),
                    "warc_date": warc_metadata.get("warc_date"),
                    "warc_record_id": warc_metadata.get("warc_record_id"),
                    "extracted_at": datetime.utcnow().isoformat() + "Z"
                }
            }
            # Write as single line JSON
            f.write(json.dumps(enriched_entity, ensure_ascii=False) + '\n')
    
    return output_file


def read_entities_from_jsonl(jsonl_file: str) -> List[Dict[str, Any]]:
    """
    Read entities from a JSONL file.
    
    Args:
        jsonl_file: Path to the JSONL file
        
    Returns:
        List of entity dictionaries
    """
    entities = []
    with open(jsonl_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                entities.append(json.loads(line))
    return entities


def get_entities_by_type(jsonl_file: str, entity_type: str) -> List[Dict[str, Any]]:
    """
    Filter entities by type from a JSONL file.
    
    Args:
        jsonl_file: Path to the JSONL file
        entity_type: Type of entity to filter (e.g., "artist", "person", "organization")
        
    Returns:
        List of entities matching the specified type
    """
    entities = read_entities_from_jsonl(jsonl_file)
    return [e for e in entities if e.get("entity", {}).get("type") == entity_type]


def deduplicate_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate entities by name (case-insensitive).
    
    Keeps the first occurrence of each unique entity name.
    
    Args:
        entities: List of entity dictionaries
        
    Returns:
        Deduplicated list of entities
    """
    seen_names = set()
    deduplicated = []
    
    for entity in entities:
        name = entity.get("entity", {}).get("name", "").lower()
        if name and name not in seen_names:
            seen_names.add(name)
            deduplicated.append(entity)
    
    return deduplicated


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        # Read and display entities from a JSONL file
        jsonl_file = sys.argv[1]
        entities = read_entities_from_jsonl(jsonl_file)
        
        print(f"Total entities: {len(entities)}")
        
        # Group by type
        by_type = {}
        for e in entities:
            entity_type = e.get("entity", {}).get("type", "unknown")
            by_type.setdefault(entity_type, []).append(e)
        
        print("\nEntities by type:")
        for entity_type, items in sorted(by_type.items()):
            print(f"  {entity_type}: {len(items)}")
        
        # Show sample entities
        print("\nSample entities:")
        for entity in entities[:5]:
            print(f"  - {entity.get('entity', {}).get('name')} ({entity.get('entity', {}).get('type')})")
            print(f"    Context: {entity.get('entity', {}).get('context', '')[:80]}...")
            print(f"    Source: {entity.get('source', {}).get('url')}")
    else:
        print("Usage: python entity_writer.py <entities.jsonl>")
