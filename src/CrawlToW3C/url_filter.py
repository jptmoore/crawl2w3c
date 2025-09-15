from urllib.parse import urlparse, parse_qs, urlunparse

SHORTENERS = {"bit.ly", "t.co", "goo.gl", "lnkd.in", "tinyurl.com"}
PAGE_EXTS = {"", ".html", ".htm", ".php", ".asp", ".aspx", ".pdf"}
SKIP_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp",
             ".mp4", ".mov", ".avi", ".mkv", ".webm", ".mp3", ".wav", ".flac",
             ".zip", ".rar", ".7z", ".tar", ".gz", ".dmg", ".exe", ".bin", ".iso"}

seen = set()

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
    u = urlparse(url)

    if u.scheme not in {"http", "https"}:
        return False 

    if u.netloc in SHORTENERS:
        return False

    if any(p in u.path.lower() for p in ["login", "signup", "admin", "cart", "checkout"]):
        return False

    q = parse_qs(u.query)
    if "q" in q or "s" in q:
        return False

    path_lower = u.path.lower()
    for ext in SKIP_EXTS:
        if path_lower.endswith(ext):
            return False

    # deduplication check
    c_url = normalise(url)
    if c_url in seen:
        return False
    seen.add(c_url)

    for ext in PAGE_EXTS:
        if path_lower.endswith(ext) or path_lower == "":
            return True

    return True
