from bs4 import BeautifulSoup

def process_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    for tag in soup(['script', 'style', 'header', 'footer', 'form']):
        tag.decompose()

    essential_tags = soup.find_all(['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'p', 'img'])

    processed_content = []

    for tag in essential_tags:
        if tag.name == 'title':
            processed_content.append(f"<title>{tag.get_text(strip=True)}</title>")
        elif tag.name.startswith('h'):
            processed_content.append(f"<{tag.name}>{tag.get_text(strip=True)}</{tag.name}>")
        elif tag.name == 'div':
            div_text = ''.join(tag.find_all(text=True, recursive=False)).strip()
            if div_text:
                processed_content.append(f"<div>{div_text}</div>")
        elif tag.name == 'p':
            processed_content.append(f"<p>{tag.get_text(strip=True)}</p>")
        elif tag.name == 'img':
            src = tag.get('src', '')
            alt = tag.get('alt', '')
            processed_content.append(f'<img src="{src}" alt="{alt}">')

    return '\n'.join(processed_content)