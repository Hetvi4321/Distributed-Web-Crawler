from bs4 import BeautifulSoup
import re
from collections import Counter
from urllib.parse import urljoin, urlparse

# Stopwords
stopwords = {"is", "the", "and", "of", "to", "in", "a"}


# ✅ Extract text
def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


# ✅ Extract clean links
def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        link = urljoin(base_url, a['href'])

        # Only valid http links
        if link.startswith("http"):
            links.append(link)

    return list(set(links))[:10]


# ✅ Process text
def process_text(text):
    text = text.lower()
    text = re.sub(r'[^a-zA-Z ]', '', text)

    words = text.split()

    cleaned = []
    for w in words:
        if w not in stopwords and len(w) > 2 and len(w) < 15:
            cleaned.append(w)

    return cleaned


# ✅ Count frequency
def count_words(words):
    return dict(Counter(words))
