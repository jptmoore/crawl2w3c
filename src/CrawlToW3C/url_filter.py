from urllib.parse import urlparse, parse_qs, urlunparse

# URL shortener domains to skip
SHORTENERS = {"bit.ly", "t.co", "goo.gl", "lnkd.in", "tinyurl.com"}

# HTML page extensions (crawler blocks other file types via blockRules)
PAGE_EXTS = {"", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp"}

# Track seen URLs for deduplication
seen = set()

def clear_seen_urls():
    """Clear the seen URLs set. Call this at the start of each pipeline run."""
    global seen
    seen.clear()

def normalise(url: str):
    u = urlparse(url)

    scheme = u.scheme.lower()
    netloc = u.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = u.path or "/"
    if path.endswith("/index.html"):
        path = path[:-11] or "/"
    elif path != "/" and path.endswith("/"):
        path = path[:-1]

    q = parse_qs(u.query)
    q = {k: v for k, v in q.items() if not k.startswith("utm_")}
    query = "&".join(f"{k}={v[0]}" for k, v in sorted(q.items()) if v)

    return urlunparse((scheme, netloc, path, "", query, ""))


def should_archive(url: str):
    """
    Heuristic filter for URLs. 
    Note: The crawler's blockRules already filter out media files (images, videos, PDFs, etc.)
    so this only needs to handle URL patterns and deduplication.
    """
    u = urlparse(url)

    # Only HTTP/HTTPS
    if u.scheme not in {"http", "https"}:
        return False 

    # Skip URL shorteners
    if u.netloc in SHORTENERS:
        return False

    # Skip common non-content pages
    if any(p in u.path.lower() for p in ["login", "signup", "admin", "cart", "checkout"]):
        return False

    # Skip search/query URLs
    q = parse_qs(u.query)
    if "q" in q or "s" in q:
        return False

    # Deduplication check
    c_url = normalise(url)
    if c_url in seen:
        return False
    seen.add(c_url)

    # Accept HTML pages (crawler blockRules already filtered out media/documents)
    path_lower = u.path.lower()
    for ext in PAGE_EXTS:
        if path_lower.endswith(ext) or path_lower == "":
            return True

    return True
