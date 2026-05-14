from typing import Optional, List, Dict
"""
Kafka Producer : envoie chaque article comme événement dans le topic news-raw
"""
import json
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "news-raw")


def get_producer() -> Optional[KafkaProducer]:
    """Crée et retourne un producer Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_block_ms=10000,
        )
        logger.info(f"Kafka producer connecté à {KAFKA_SERVERS}")
        return producer
    except KafkaError as e:
        logger.error(f"Impossible de connecter Kafka : {e}")
        return None


def send_articles(producer: KafkaProducer, articles: List[Dict]) -> int:
    """
    Envoie une liste d'articles dans le topic Kafka.
    Retourne le nombre d'articles envoyés avec succès.
    """
    if not producer or not articles:
        return 0

    sent = 0
    for article in articles:
        try:
            # Clé = url_hash pour garantir l'ordre par article
            future = producer.send(
                topic=TOPIC_RAW,
                key=article.get("url_hash", ""),
                value=article,
            )
            future.get(timeout=10)  # Attendre la confirmation
            sent += 1
        except KafkaError as e:
            logger.warning(f"Échec envoi article '{article.get('title', '')}' : {e}")

    producer.flush()
    logger.info(f"Kafka : {sent}/{len(articles)} articles envoyés dans '{TOPIC_RAW}'")
    return sent
