import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urljoin
import os

# 🔥 Each worker gets a unique ID
WORKER_ID = os.getenv("WORKER_ID", "2")

DISTRIBUTOR_URL = "http://10.100.109.25:5000"
INDEXER_URL = "http://172.27.225.21:5002/index"


def log(msg):
    print(f"[Worker {WORKER_ID}] {msg}")


def get_url_from_distributor():
    try:
        response = requests.get(f"{DISTRIBUTOR_URL}/get_url")
        data = response.json()

        if data["status"] == "success":
            return data["url"]
        else:
            return None

    except Exception as e:
        log(f"Error getting URL: {e}")
        return None


def send_to_indexer(url, html):
    try:
        payload = {
            "url": url,
            "html": html
        }

        response = requests.post(INDEXER_URL, json=payload)

        if response.status_code == 200:
            log(f"Indexed: {url}")
        else:
            log(f"Indexer error {response.status_code} for {url}")

    except Exception as e:
        log(f"Error sending to indexer: {e}")


def crawl(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=5)

        if response.status_code != 200:
            log(f"Failed: {url}")
            return [], None

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        base_domain = urlparse(url).netloc
        links = []

        for a_tag in soup.find_all("a", href=True):
            link = urljoin(url, a_tag["href"])
            if urlparse(link).netloc == base_domain:
                links.append(link)

        links = list(set(links))[:10]

        log(f"Crawled: {url} | Links: {len(links)}")
        return links, html

    except Exception as e:
        log(f"Error crawling {url}: {e}")
        return [], None


def send_links_to_distributor(links):
    try:
        if links:
            requests.post(
                f"{DISTRIBUTOR_URL}/add_url",
                json={"urls": links}
            )
            log(f"Sent {len(links)} links")

    except Exception as e:
        log(f"Error sending links: {e}")


def worker_loop():
    while True:
        url = get_url_from_distributor()

        if url:
            links, html = crawl(url)

            if html:
                send_to_indexer(url, html)

            send_links_to_distributor(links)

            time.sleep(1)
        else:
            log("No URLs, waiting...")
            time.sleep(3)


if __name__ == "__main__":
    worker_loop()