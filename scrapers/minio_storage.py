from typing import List, Dict, Optional
"""
MinIO Storage : sauvegarde les articles dans la couche Bronze du Data Lake
Structure : bronze/YYYY/MM/DD/<source>/<url_hash>.json
"""
import json
import logging
import os
from datetime import datetime
from io import BytesIO
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_BRONZE   = os.getenv("MINIO_BUCKET_BRONZE", "bronze")


def get_minio_client() -> Minio:
    """Crée et retourne un client MinIO."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,  # HTTP en local
    )


def save_articles_to_bronze(articles: List[Dict]) -> int:
    """
    Sauvegarde une liste d'articles dans la couche Bronze de MinIO.
    Retourne le nombre d'articles sauvegardés.
    """
    if not articles:
        return 0

    client = get_minio_client()
    saved = 0
    now = datetime.utcnow()

    for article in articles:
        try:
            source = article.get("source", "unknown").lower().replace(" ", "_")
            url_hash = article.get("url_hash", "unknown")

            # Chemin de partitionnement par date
            path = (
                f"{now.year}/{now.month:02d}/{now.day:02d}"
                f"/{source}/{url_hash}.json"
            )

            # Ajouter le chemin MinIO à l'article
            article["minio_path"] = f"{BUCKET_BRONZE}/{path}"

            # Sérialiser en JSON
            data = json.dumps(article, ensure_ascii=False, indent=2).encode("utf-8")
            stream = BytesIO(data)

            client.put_object(
                bucket_name=BUCKET_BRONZE,
                object_name=path,
                data=stream,
                length=len(data),
                content_type="application/json",
            )
            saved += 1

        except S3Error as e:
            logger.warning(f"MinIO erreur article {article.get('url_hash', '')} : {e}")
        except Exception as e:
            logger.warning(f"Erreur sauvegarde article : {e}")

    logger.info(f"MinIO Bronze : {saved}/{len(articles)} articles sauvegardés")
    return saved


def list_bronze_articles(date: datetime = None) -> List[str]:
    """Liste les fichiers dans le bucket bronze pour une date donnée."""
    client = get_minio_client()
    date = date or datetime.utcnow()
    prefix = f"{date.year}/{date.month:02d}/{date.day:02d}/"

    try:
        objects = client.list_objects(BUCKET_BRONZE, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        logger.error(f"Erreur listing MinIO bronze : {e}")
        return []
