#!/usr/bin/env python3
"""
Upload Existing Results to Miiify Server

This script uploads an existing results_collection.json to Miiify server
without running the LLM pipeline. Useful for re-uploading results or
testing the Miiify integration.
"""

import json
import os
import sys
import time
import requests
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from CrawlToW3C.miiify_client import MiiifyClient, upload_collection_to_miiify


def wait_for_miiify_server(base_url: str = "http://localhost:10000", max_attempts: int = 30):
    """
    Wait for Miiify server to be ready.
    
    Args:
        base_url: Base URL of the Miiify server
        max_attempts: Maximum number of connection attempts
    """
    print(f"⏳ Waiting for Miiify server at {base_url}...")
    
    for attempt in range(max_attempts):
        try:
            # Try to connect to the server
            response = requests.get(f"{base_url}/", timeout=5)
            if response.status_code == 200 and response.text.strip() == "OK":
                print(f"✅ Miiify server is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"⏳ Attempt {attempt + 1}/{max_attempts} - Server not ready, waiting...")
        time.sleep(2)
    
    print(f"❌ ERROR: Miiify server at {base_url} is not responding after {max_attempts} attempts")
    return False


def main():
    """Main function to upload existing results to Miiify."""
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scripts/upload_existing_results.py <results_collection.json>")
        print("  python scripts/upload_existing_results.py --default")
        print("")
        print("Examples:")
        print("  # Upload specific file")
        print("  python scripts/upload_existing_results.py src/CrawlToW3C/results/results_collection.json")
        print("  # Upload default file")
        print("  python scripts/upload_existing_results.py --default")
        sys.exit(1)
    
    # Determine results file path
    if sys.argv[1] == "--default":
        results_file = "src/CrawlToW3C/results/results_collection.json"
        miiify_url = "http://localhost:10000"  # Local development
        print(f"📁 Using default results file: {results_file}")
    else:
        results_file = sys.argv[1]
        miiify_url = "http://localhost:10000"  # Local development
        print(f"📁 Using specified results file: {results_file}")
    
    # Check if file exists
    if not os.path.exists(results_file):
        print(f"❌ ERROR: Results file not found: {results_file}")
        sys.exit(1)
    
    # Wait for Miiify server to be ready
    if not wait_for_miiify_server(miiify_url):
        sys.exit(1)
    
    # Load the results collection
    try:
        with open(results_file, 'r') as f:
            collection_data = json.load(f)
        print(f"📋 Loaded collection: {collection_data.get('label', 'Unknown')}")
        
        # Show collection stats
        total_annotations = sum(len(page.get('items', [])) for page in collection_data.get('items', []))
        print(f"📊 Collection contains {len(collection_data.get('items', []))} pages with {total_annotations} total annotations")
        
    except FileNotFoundError:
        print(f"❌ ERROR: Results file not found: {results_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in results file: {e}")
        sys.exit(1)
    
    # Create Miiify client and upload
    try:
        client = MiiifyClient(base_url=miiify_url)
        print("📡 Uploading collection to Miiify server...")
        
        results = upload_collection_to_miiify(collection_data, client)
        
        # Print results summary
        print("\n" + "="*60)
        print("📈 UPLOAD RESULTS SUMMARY")
        print("="*60)
        print(f"Container created: {results['container_created']}")
        print(f"Annotations uploaded: {results['annotations_uploaded']}")
        
        if results['errors']:
            print(f"❌ Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                print(f"  • {error}")
            print("="*60)
            sys.exit(1)
        else:
            print("✅ Upload completed successfully!")
            print("="*60)
            
    except Exception as e:
        print(f"❌ ERROR: Failed to upload to Miiify server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()