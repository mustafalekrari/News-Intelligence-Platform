from typing import List, Dict, Optional
"""
Scraper : BBC News (actualités internationales en anglais)
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import hashlib

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Apple M3) AppleWebKit/537.36 Chrome/120.0",
    "Accept-Language": "en-GB,en;q=0.9",
}

BASE_URL = "https://www.bbc.com"

SECTIONS = [
    "/news/world",
    "/news/technology",
    "/news/science_and_environment",
    "/news/business",
]


def scrape_bbc() -> List[Dict]:
    """Collecte les articles BBC News depuis les sections principales."""
    articles = []

    for section in SECTIONS:
        try:
            url = f"{BASE_URL}{section}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Liens d'articles BBC
            links = set()
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if "/news/" in href and href.count("-") > 3:
                    if href.startswith("/"):
                        href = BASE_URL + href
                    if href.startswith("https://www.bbc.com/news/"):
                        links.add(href)

            for link in list(links)[:8]:
                article = _scrape_article(link, section.split("/")[-1])
                if article:
                    articles.append(article)

        except Exception as e:
            logger.warning(f"BBC section {section} : {e}")

    logger.info(f"BBC : {len(articles)} articles collectés")
    return articles


def _scrape_article(url: str, category: str) -> Optional[Dict]:
    """Scrape le contenu d'un article BBC."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        title = soup.select_one("h1")
        # BBC utilise des data-component pour le contenu
        paragraphs = soup.select("[data-component='text-block'] p")
        if not paragraphs:
            paragraphs = soup.select("article p")

        author_el = soup.select_one("[class*='TextContributorName']")
        date_el = soup.select_one("time")

        if not title or not paragraphs:
            return None

        content = " ".join(p.get_text(strip=True) for p in paragraphs)

        if len(content) < 100:
            return None

        published = ""
        if date_el:
            published = date_el.get("datetime", date_el.get_text(strip=True))

        return {
            "url_hash": hashlib.sha256(url.encode()).hexdigest(),
            "url": url,
            "title": title.get_text(strip=True),
            "author": author_el.get_text(strip=True) if author_el else "BBC News",
            "content": content,
            "category": category,
            "source": "BBC News",
            "country": "GB",
            "language": "en",
            "published_at": published or datetime.now().isoformat(),
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.debug(f"Article BBC {url} : {e}")
        return None
