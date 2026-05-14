from typing import List, Dict, Optional
"""
Scraper RSS : Al Jazeera + Reuters
Plus fiable que le scraping HTML — flux RSS officiels
"""
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import hashlib
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Apple M3) AppleWebKit/537.36 Chrome/120.0",
}

RSS_FEEDS = {
    "Al Jazeera": {
        "feeds": [
            "https://www.aljazeera.com/xml/rss/all.xml",
            "https://www.aljazeera.com/xml/rss/world.xml",
        ],
        "country": "QA",
        "language": "en",
    },
    "Reuters": {
        "feeds": [
            "https://feeds.reuters.com/reuters/topNews",
            "https://feeds.reuters.com/reuters/worldNews",
            "https://feeds.reuters.com/reuters/technologyNews",
        ],
        "country": "GB",
        "language": "en",
    },
    "CNN": {
        "feeds": [
            "http://rss.cnn.com/rss/edition.rss",
            "http://rss.cnn.com/rss/edition_world.rss",
            "http://rss.cnn.com/rss/edition_technology.rss",
        ],
        "country": "US",
        "language": "en",
    },
}


def scrape_rss_sources() -> List[Dict]:
    """Collecte les articles via RSS pour Al Jazeera et Reuters."""
    all_articles = []

    for source_name, config in RSS_FEEDS.items():
        articles = _scrape_source(source_name, config)
        all_articles.extend(articles)
        logger.info(f"{source_name} : {len(articles)} articles collectés")

    return all_articles


def _scrape_source(source_name: str, config: dict) -> List[Dict]:
    """Collecte les articles d'une source via ses flux RSS."""
    articles = []
    seen_urls = set()

    for feed_url in config["feeds"]:
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries[:10]:
                url = entry.get("link", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # Extraire la date
                published = _parse_date(entry)

                # Extraire le contenu depuis le RSS
                content = _extract_content(entry)

                # Si contenu trop court, scraper la page directement
                if len(content) < 200:
                    content = _fetch_article_content(url) or content

                if len(content) < 100:
                    continue

                category = ""
                if entry.get("tags"):
                    category = entry.tags[0].get("term", "")

                articles.append({
                    "url_hash": hashlib.sha256(url.encode()).hexdigest(),
                    "url": url,
                    "title": entry.get("title", "").strip(),
                    "author": _extract_author(entry, source_name),
                    "content": content,
                    "category": category,
                    "source": source_name,
                    "country": config["country"],
                    "language": config["language"],
                    "published_at": published,
                    "scraped_at": datetime.utcnow().isoformat(),
                })

        except Exception as e:
            logger.warning(f"{source_name} flux {feed_url} : {e}")

    return articles


def _parse_date(entry) -> str:
    """Parse la date depuis une entrée RSS."""
    try:
        if entry.get("published"):
            return parsedate_to_datetime(entry.published).isoformat()
    except Exception:
        pass
    try:
        if entry.get("updated"):
            return parsedate_to_datetime(entry.updated).isoformat()
    except Exception:
        pass
    return datetime.utcnow().isoformat()


def _extract_content(entry) -> str:
    """Extrait le contenu texte depuis une entrée RSS."""
    # Essayer content:encoded en premier
    if entry.get("content"):
        raw = entry.content[0].get("value", "")
        return BeautifulSoup(raw, "lxml").get_text(separator=" ", strip=True)

    # Sinon le summary
    if entry.get("summary"):
        return BeautifulSoup(entry.summary, "lxml").get_text(separator=" ", strip=True)

    return ""


def _fetch_article_content(url: str) -> Optional[str]:
    """Scrape le contenu complet d'un article si le RSS est trop court."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Supprimer nav, footer, ads
        for tag in soup(["nav", "footer", "script", "style", "aside", "header"]):
            tag.decompose()

        # Chercher le contenu principal
        for selector in ["article", "main", "[class*='article-body']", "[class*='story-body']"]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(separator=" ", strip=True)
                if len(text) > 200:
                    return text

        return None
    except Exception:
        return None


def _extract_author(entry, default: str) -> str:
    """Extrait l'auteur depuis une entrée RSS."""
    if entry.get("author"):
        return entry.author
    if entry.get("authors"):
        return entry.authors[0].get("name", default)
    return default
