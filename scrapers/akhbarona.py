from typing import List, Dict, Optional
"""
Scraper : Akhbarona.com (actualités marocaines en arabe)
"""
import hashlib
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Apple M3) AppleWebKit/537.36 Chrome/120.0",
    "Accept-Language": "ar,fr;q=0.9",
}

BASE_URL = "https://www.akhbarona.com"

SECTIONS = [
    "/politics/",
    "/economy/",
    "/society/",
    "/sports/",
    "/world/",
]


def scrape_akhbarona() -> List[Dict]:
    """Collecte les articles Akhbarona depuis les sections principales."""
    articles = []

    for section in SECTIONS:
        try:
            url = f"{BASE_URL}{section}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Liens d'articles
            links = set()
            for a in soup.select("h2 a, h3 a, .article-title a"):
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith("/"):
                    href = BASE_URL + href
                if BASE_URL in href and href != url:
                    links.add(href)

            for link in list(links)[:8]:
                article = _scrape_article(link, section.strip("/"))
                if article:
                    articles.append(article)

        except Exception as e:
            logger.warning(f"Akhbarona section {section} : {e}")

    logger.info(f"Akhbarona : {len(articles)} articles collectés")
    return articles


def _scrape_article(url: str, category: str) -> Optional[Dict]:
    """Scrape le contenu d'un article individuel."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        title = soup.select_one("h1")
        content_div = soup.select_one("div.article-body, div.content-article, article")
        date_el = soup.select_one("time, span.date, .article-date")
        author_el = soup.select_one(".author, .article-author, span[class*='author']")

        if not title:
            return None

        if content_div:
            for tag in content_div(["script", "style", "aside"]):
                tag.decompose()
            content = content_div.get_text(separator=" ", strip=True)
        else:
            content = ""

        if len(content) < 100:
            return None

        published = ""
        if date_el:
            published = date_el.get("datetime", date_el.get_text(strip=True))

        return {
            "url_hash": hashlib.sha256(url.encode()).hexdigest(),
            "url": url,
            "title": title.get_text(strip=True),
            "author": author_el.get_text(strip=True) if author_el else "Akhbarona",
            "content": content,
            "category": category,
            "source": "Akhbarona",
            "country": "MA",
            "language": "ar",
            "published_at": published or datetime.utcnow().isoformat(),
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.debug(f"Article Akhbarona {url} : {e}")
        return None
