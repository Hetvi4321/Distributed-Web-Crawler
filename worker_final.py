import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, urljoin
import os

# 🔥 Unique Worker ID
WORKER_ID = os.getenv("WORKER_ID", "1")

# 🔗 Service URLs
DISTRIBUTOR_URL = "http://10.201.13.25:5000"
INDEXER_URL = "http://10.201.13.21:5002/index"


def log(msg):
    print(f"[Worker {WORKER_ID}] {msg}")


# ✅ 1. Get URL from Distributor
def get_url_from_distributor():
    try:
        response = requests.get(f"{DISTRIBUTOR_URL}/get_url", timeout=5)
        data = response.json()

        if data.get("status") == "success":
            return data.get("url")
        return None

    except Exception as e:
        log(f"Error getting URL: {e}")
        return None


# ✅ 2. Send crawled data to Indexer
def send_to_indexer(url, html):
    payload = {
        "url": url,
        "html": html[:200000]   # 🔥 limit size
    }

    try:
        response = requests.post(
            INDEXER_URL,
            json=payload,
            timeout=15   # ✅ increase timeout
        )

        if response.status_code == 200:
            log(f"Indexed: {url}")
            return True

    except Exception as e:
        log(f"Error sending to indexer: {e}")

    return False

# ✅ 3. Crawl webpage
def crawl(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=5)

        if response.status_code != 200:
            log(f"Failed to fetch: {url}")
            return [], None

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        base_domain = urlparse(url).netloc
        links = []

        for a_tag in soup.find_all("a", href=True):
            link = urljoin(url, a_tag["href"])

            # Stay within same domain (optional control)
            if urlparse(link).netloc == base_domain:
                links.append(link)

        # Remove duplicates + limit
        links = list(set(links))[:10]

        log(f"Crawled: {url} | Extracted links: {len(links)}")
        return links, html

    except Exception as e:
        log(f"Error crawling {url}: {e}")
        return [], None


# ✅ 4. Send new links back to Distributor
def send_links_to_distributor(links):
    try:
        if links:
            requests.post(
                f"{DISTRIBUTOR_URL}/add_url",
                json={"urls": links},
                timeout=5
            )
            log(f"Sent {len(links)} links to distributor")

    except Exception as e:
        log(f"Error sending links: {e}")


# ✅ 5. Mark URL as DONE
def mark_done(url):
    try:
        requests.post(
            f"{DISTRIBUTOR_URL}/done",
            json={
                "url": url,
                "worker_id": WORKER_ID
            },
            timeout=5
        )
        log(f"Marked done: {url}")

    except Exception as e:
        log(f"Error marking done: {e}")


# ✅ 6. Main Worker Loop
def worker_loop():
    while True:
        url = get_url_from_distributor()

        if url:
            log(f"Processing: {url}")

            links, html = crawl(url)

            # ✅ Only mark done if successful
            if html:
                indexed = send_to_indexer(url, html)

                if indexed:
                    send_links_to_distributor(links)
                    mark_done(url)
                else:
                    log(f"Skipping mark_done (indexing failed): {url}")

            else:
                log(f"Skipping mark_done (crawl failed): {url}")

            time.sleep(1)

        else:
            log("No URLs available, waiting...")
            time.sleep(3)


if __name__ == "__main__":
    worker_loop()