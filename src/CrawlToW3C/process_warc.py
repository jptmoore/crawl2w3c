import os
from warcio.archiveiterator import ArchiveIterator

def get_warc_file_paths():
    archive_path = os.path.join(
        "crawls",
        "collections",
        "one",
        "archive"
    )

    warc_filepaths = [
        os.path.join(archive_path, f)
        for f in os.listdir(archive_path)
        if f.endswith(".warc.gz") or f.endswith(".warc")
    ]

    return warc_filepaths


def iter_warc_records(warc_filepaths: str):
    for warc_filepath in warc_filepaths:
        with open(warc_filepath, "rb") as f:
            for record in ArchiveIterator(f):
                yield record


def get_urls(warc_filepaths: str):
    "Returns a list of URLs from a WARC file that correspond to HTML response records"
    urls = []
    for record in iter_warc_records(warc_filepaths):
        if record.rec_type != "response":
            continue

        http_headers = record.http_headers
        if not http_headers:
            continue

        content_type = http_headers.get_header("content-type")
        if not content_type or "text/html" not in content_type:
            continue

        url = record.rec_headers.get_header("WARC-Target-URI")
        if url:
            urls.append(url)

    return urls


def iter_html_responses(warc_filepaths: str):
    """Iterate over HTML responses in WARC file. Yields HTML and URL."""
    for record in iter_warc_records(warc_filepaths):
        if record.rec_type != "response":
            continue

        http_headers = record.http_headers
        if not http_headers:
            continue

    
        content_type = http_headers.get_header("content-type")
        if not content_type or "text/html" not in content_type:
            continue

        payload = record.content_stream().read()
        html = payload.decode("utf-8", errors="ignore")
        
        url = record.rec_headers.get_header("WARC-Target-URI")

        yield url, html



if __name__ == "__main__":
    warcs = get_warc_file_paths()
    print(warcs)
    urls = get_urls(warc_filepaths=warcs)
    print(len(urls))
    count = 0
    for warc in warcs:
        with open(warc, "rb") as stream:
            for record in ArchiveIterator(stream):
                count += 1

    print(count)
