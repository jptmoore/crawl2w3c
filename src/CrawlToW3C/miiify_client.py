"""
Miiify Annotation Server Client

This module provides a client for interacting with a Miiify annotation server
to create containers and upload W3C Web Annotations.
"""

import json
import hashlib
import requests
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin


class MiiifyClient:
    """Client for interacting with Miiify annotation server."""
    
    def __init__(self, base_url: str = "http://miiify:10000"):
        """
        Initialize Miiify client.
        
        Args:
            base_url: Base URL of the Miiify annotation server
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def create_container(self, container_slug: str, container_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an annotation container on the Miiify server.
        
        Args:
            container_slug: Unique identifier for the container
            container_data: W3C AnnotationCollection data
            
        Returns:
            Response from the server
        """
        url = urljoin(self.base_url, "/annotations/")
        headers = self.session.headers.copy()
        headers['Slug'] = container_slug
        
        print(f"🔍 Creating container with slug: {container_slug}")
        print(f"🔍 Container data: {json.dumps(container_data, indent=2)}")
        print(f"🔍 Headers: {headers}")
        
        try:
            response = self.session.post(url, json=container_data, headers=headers)
            print(f"🔍 Response status: {response.status_code}")
            print(f"🔍 Response text: {response.text}")
            
            if response.status_code == 400 and "container exists" in response.text.lower():
                print(f"🔄 Container {container_slug} already exists, attempting to delete and recreate...")
                delete_success = self.delete_container(container_slug)
                
                if delete_success:
                    # Try creating again after successful deletion
                    print(f"🔄 Retrying container creation after deletion...")
                    response = self.session.post(url, json=container_data, headers=headers)
                    print(f"🔍 Retry response status: {response.status_code}")
                    print(f"🔍 Retry response text: {response.text}")
                else:
                    print(f"⚠️ Could not delete existing container. Container may already exist and deletion is not supported.")
                    print(f"💡 Continuing with existing container: {container_slug}")
                    # Don't raise error - use existing container
                    return {"message": f"Using existing container {container_slug}"}
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error creating container {container_slug}: {e}")
            print(f"Response content: {response.text if 'response' in locals() else 'No response'}")
            raise
    
    def upload_annotation(self, container_slug: str, annotation_slug: str, 
                         annotation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upload an annotation to a container using the annotation slug.
        
        Args:
            container_slug: Container identifier
            annotation_slug: Unique identifier for the annotation (derived from annotation ID)
            annotation_data: W3C Annotation data
            
        Returns:
            Response from the server
        """
        url = urljoin(self.base_url, f"/annotations/{container_slug}/")
        headers = self.session.headers.copy()
        headers['Slug'] = annotation_slug
        
        # Remove ID and created from annotation data - server will create these
        clean_annotation = annotation_data.copy()
        if 'id' in clean_annotation:
            original_id = clean_annotation.pop('id')
            print(f"🔍 Removed ID '{original_id}' from annotation, using Slug: {annotation_slug}")
        if 'created' in clean_annotation:
            original_created = clean_annotation.pop('created')
            print(f"🔍 Removed created timestamp '{original_created}' - server will create its own")
        
        print(f"🔍 Uploading annotation to: {url}")
        print(f"🔍 Annotation slug: {annotation_slug}")
        print(f"🔍 Annotation data: {json.dumps(clean_annotation, indent=2)}")
        print(f"🔍 Headers: {headers}")
        
        try:
            response = self.session.post(url, json=clean_annotation, headers=headers)
            print(f"🔍 Response status: {response.status_code}")
            print(f"🔍 Response text: {response.text}")
            
            if response.status_code == 201:
                print(f"✅ Successfully uploaded annotation {annotation_slug} (201 Created)")
                return response.json()
            elif response.status_code == 400:
                print(f"❌ Bad Request (400) for annotation {annotation_slug}")
                print(f"❌ Error details: {response.text}")
                raise requests.exceptions.HTTPError(f"400 Bad Request: {response.text}")
            else:
                response.raise_for_status()
                return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error uploading annotation {annotation_slug}: {e}")
            print(f"❌ Response content: {response.text if 'response' in locals() else 'No response'}")
            raise
    
    def get_container(self, container_slug: str) -> Dict[str, Any]:
        """
        Retrieve a container from the server.
        
        Args:
            container_slug: Container identifier
            
        Returns:
            Container data
        """
        url = urljoin(self.base_url, f"/annotations/{container_slug}/")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving container {container_slug}: {e}")
            raise
    

    
    def delete_container(self, container_slug: str) -> bool:
        """
        Delete a container from the server.
        
        Args:
            container_slug: Container identifier
            
        Returns:
            True if deleted successfully, False if not found
        """
        url = urljoin(self.base_url, f"/annotations/{container_slug}")
        
        print(f"🗑️ Attempting to delete container at: {url}")
        
        try:
            response = self.session.delete(url)
            print(f"🔍 Delete response status: {response.status_code}")
            print(f"🔍 Delete response text: {response.text}")
            
            if response.status_code == 404:
                print(f"🔍 Container {container_slug} does not exist (404)")
                return False
            elif response.status_code == 405:
                print(f"⚠️ DELETE method not allowed - container deletion not supported")
                return False
            elif response.status_code in [200, 204]:
                print(f"✅ Successfully deleted container: {container_slug} (status: {response.status_code})")
                return True
            else:
                response.raise_for_status()
                print(f"✅ Successfully deleted container: {container_slug}")
                return True
        except requests.exceptions.RequestException as e:
            print(f"❌ Error deleting container {container_slug}: {e}")
            print(f"Delete response content: {response.text if 'response' in locals() else 'No response'}")
            return False


def extract_slug_from_annotation_id(annotation_id: str) -> str:
    """
    Extract a slug from an annotation ID.
    
    For URN-based IDs like 'urn:sha256:abc123...', use the full hash part.
    For other IDs, create a hash-based slug.
    
    Args:
        annotation_id: The annotation ID
        
    Returns:
        A suitable slug for the Miiify server
    """
    if annotation_id.startswith('urn:sha256:'):
        # Extract the full hash part after 'urn:sha256:'
        return annotation_id.split(':', 2)[2]  # Full hash as slug
    else:
        # Create a hash-based slug for other ID formats
        hash_obj = hashlib.sha256(annotation_id.encode('utf-8'))
        return hash_obj.hexdigest()


def create_container_slug(collection_id: str, warc_filename: str = None) -> str:
    """
    Create a container slug from collection ID and optional WARC filename.
    
    Args:
        collection_id: The collection ID
        warc_filename: Optional WARC filename for additional context
        
    Returns:
        A suitable container slug
    """
    if warc_filename:
        # Use WARC filename (without extension) as primary slug component
        base_name = warc_filename.replace('.warc.gz', '').replace('.warc', '')
        return f"crawl2w3c-{base_name}"
    else:
        # Fallback to hash of collection ID
        hash_obj = hashlib.sha256(collection_id.encode('utf-8'))
        return f"crawl2w3c-{hash_obj.hexdigest()[:12]}"


def create_basic_container(label: str) -> Dict[str, Any]:
    """
    Create a basic IIIF Presentation API AnnotationCollection container.
    
    Args:
        label: Human-readable label for the container
        
    Returns:
        Basic container structure for Miiify (without ID - server creates it)
    """
    return {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "type": "AnnotationCollection", 
        "label": label
    }


def upload_collection_to_miiify(collection_data: Dict[str, Any], 
                               miiify_client: MiiifyClient) -> Dict[str, Any]:
    """
    Upload a complete W3C AnnotationCollection to Miiify server.
    
    Args:
        collection_data: W3C AnnotationCollection JSON data
        miiify_client: Configured Miiify client
        
    Returns:
        Summary of upload results
    """
    results = {
        'container_created': False,
        'annotations_uploaded': 0,
        'errors': []
    }
    
    try:
        # Extract WARC filename from label if available
        warc_filename = None
        label = collection_data.get('label', '')
        if ' - ' in label and label.endswith('.warc.gz'):
            warc_filename = label.split(' - ')[-1]
        
        # Create container slug
        container_slug = create_container_slug(
            collection_data['id'], 
            warc_filename
        )
        
        # Create container metadata in IIIF Presentation API format
        container_metadata = {
            "@context": "http://iiif.io/api/presentation/3/context.json",
            "type": "AnnotationCollection",
            "label": collection_data.get('label', 'A Container for Web Annotations')
        }
        
        # Create the container (will delete and recreate if exists)
        print(f"🏗️ Creating container: {container_slug}")
        miiify_client.create_container(container_slug, container_metadata)
        results['container_created'] = True
        print(f"✅ Container created successfully: {container_slug}")
        
        # Upload individual annotations from all pages
        total_pages = len(collection_data.get('items', []))
        print(f"📄 Found {total_pages} annotation pages in collection")
        
        for page_idx, page in enumerate(collection_data.get('items', []), 1):
            page_annotations = page.get('items', [])
            print(f"📄 Page {page_idx}/{total_pages}: {len(page_annotations)} annotations")
            
            for ann_idx, annotation in enumerate(page_annotations, 1):
                print(f"\n📤 Processing annotation {ann_idx}/{len(page_annotations)} from page {page_idx}")
                print(f"🔍 Annotation keys: {list(annotation.keys())}")
                print(f"🔍 Annotation type: {annotation.get('type', 'MISSING')}")
                print(f"🔍 Annotation ID: {annotation.get('id', 'MISSING')}")
                
                if 'id' not in annotation:
                    print(f"❌ Skipping annotation without ID")
                    continue
                    
                annotation_slug = extract_slug_from_annotation_id(annotation['id'])
                
                print(f"� Original annotation ID: {annotation['id']}")
                print(f"�📤 Uploading annotation with slug: {annotation_slug}")
                print(f"🔍 Expected server ID: http://miiify:10000/annotations/{container_slug}/{annotation_slug}")
                miiify_client.upload_annotation(
                    container_slug, 
                    annotation_slug, 
                    annotation
                )
                results['annotations_uploaded'] += 1
        
        print(f"✅ Successfully uploaded {results['annotations_uploaded']} annotations to container {container_slug}")
        
    except Exception as e:
        error_msg = f"Error uploading collection: {e}"
        print(error_msg)
        results['errors'].append(error_msg)
    
    return results


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) == 1:
        # Demo: Create a basic container
        print("Creating basic container demo...")
        client = MiiifyClient()
        container_data = create_basic_container("Demo Container for Web Annotations")
        
        try:
            result = client.create_container("demo-container", container_data)
            print("Container created:", json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error: {e}")
            
    elif len(sys.argv) == 2:
        collection_file = sys.argv[1]
        
        # Load collection data
        with open(collection_file, 'r') as f:
            collection_data = json.load(f)
        
        # Create client and upload
        client = MiiifyClient()
        results = upload_collection_to_miiify(collection_data, client)
        
        print("Upload results:", json.dumps(results, indent=2))
    else:
        print("Usage:")
        print("  python miiify_client.py                          # Create demo container")
        print("  python miiify_client.py <results_collection.json> # Upload collection")