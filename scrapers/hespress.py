from typing import List, Dict, Optional
"""
Scraper : Hespress.com (actualités marocaines en arabe)
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import hashlib

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Apple M3) AppleWebKit/537.36 Chrome/120.0",
    "Accept-Language": "ar,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://hespress.com"

CATEGORIES = [
    "/politique",
    "/societe", 
    "/economie",
    "/sport",
    "/monde",
]


def scrape_hespress() -> List[Dict]:
    """Collecte les articles Hespress depuis les catégories principales."""
    articles = []

    for cat in CATEGORIES:
        try:
            url = f"{BASE_URL}{cat}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Articles dans les cards de la page catégorie
            cards = soup.select("div.overlay a")[:10]  # max 10 par catégorie

            for card in cards:
                article_url = card.get("href", "")
                if not article_url.startswith("http"):
                    article_url = BASE_URL + article_url

                article = _scrape_article(article_url, cat.strip("/"))
                if article:
                    articles.append(article)

        except Exception as e:
            logger.warning(f"Hespress catégorie {cat} : {e}")

    logger.info(f"Hespress : {len(articles)} articles collectés")
    return articles


def _scrape_article(url: str, category: str) -> Optional[Dict]:
    """Scrape le contenu d'un article individuel."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        title = soup.select_one("h1.post-title")
        content_div = soup.select_one("div.article-content")
        author = soup.select_one("span.author-name")
        date_el = soup.select_one("span.post-date")

        if not title or not content_div:
            return None

        content = content_div.get_text(separator=" ", strip=True)

        # Ignorer les articles trop courts
        if len(content) < 100:
            return None

        return {
            "url_hash": hashlib.sha256(url.encode()).hexdigest(),
            "url": url,
            "title": title.get_text(strip=True),
            "author": author.get_text(strip=True) if author else "Hespress",
            "content": content,
            "category": category,
            "source": "Hespress",
            "country": "MA",
            "language": "ar",
            "published_at": date_el.get_text(strip=True) if date_el else datetime.now().isoformat(),
            "scraped_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.debug(f"Article Hespress {url} : {e}")
        return None
