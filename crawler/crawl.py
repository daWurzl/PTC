"""
Einfacher asynchroner Webcrawler, der Title und Link in eine CSV schreibt.
- Nur öffentlich zugängliche Seiten
- Keine externen Dienste (Redis/DB)
- Resource-Limits (CPU und Arbeitsspeicher) via Python `resource` (nur auf Unix)
- Konfiguration via Pydantic und .env
- Asyncio + aiohttp
- Verbesserungen: Portabilität, Verzeichnisprüfung, Error-Counter
"""
import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
# Beachte: urlparse wird hier nicht benötigt, daher import entfernt
from urllib.parse import urlparse

# Resource-Limits nur auf Unix-Systemen
if os.name == 'posix':
    import resource
    # Max CPU-Zeit in Sekunden (hier 300s)
    resource.setrlimit(resource.RLIMIT_CPU, (300, 300))
    # Max virtueller Speicher (hier 500 MB)
    memory_limit = 500 * 1024 * 1024  # Bytes
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from pydantic import BaseSettings, Field

# --- Konfiguration via Pydantic ---
class Settings(BaseSettings):
    START_URLS: List[str] = Field([
        "https://www.example.com/",
        # weitere URLs hier
    ])
    USER_AGENTS: List[str] = Field([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...",
    ])
    OUTPUT_CSV: str = Field("data/results.csv")
    CONCURRENT_REQUESTS: int = Field(5)
    REQUEST_TIMEOUT: int = Field(15)
    class Config:
        env_file = ".env"

settings = Settings()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Fehlerzähler ---
error_count = 0

async def fetch_page(session: aiohttp.ClientSession, url: str, user_agent: str) -> Optional[str]:
    """Lädt HTML-Inhalt asynchron mit Timeout und User-Agent."""
    global error_count
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(url, headers=headers, timeout=ClientTimeout(total=settings.REQUEST_TIMEOUT)) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as e:
        error_count += 1
        logger.warning(f"Fehler beim Abruf von {url}: {e}")
        return None

async def process_url(
    session: aiohttp.ClientSession,
    url: str
) -> Optional[Tuple[str, str]]:
    """Verarbeitet eine URL: lädt HTML, parst Title und liefert (title, url)."""
    user_agent = random.choice(settings.USER_AGENTS)
    html = await fetch_page(session, url, user_agent)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else "Ohne Titel"
    return title, url

async def crawl():
    """Hauptfunktion: crawlt Start-URLs und speichert Ergebnisse in CSV."""
    # Verzeichnis prüfen
    out_dir = os.path.dirname(settings.OUTPUT_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    results: List[Tuple[str, str]] = []

    connector = aiohttp.TCPConnector(limit=settings.CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_url(session, url) for url in settings.START_URLS]
        for future in asyncio.as_completed(tasks):
            res = await future
            if res:
                results.append(res)
                logger.info(f"Gefunden: {res[0]} ({res[1]})")

    # Ergebnisse in CSV schreiben
    with open(settings.OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "URL"])
        writer.writerows(results)

    logger.info(f"Crawling abgeschlossen: {len(results)} Einträge, {error_count} Fehler")

if __name__ == "__main__":
    try:
        asyncio.run(crawl())
    except Exception as e:
        logger.error(f"Unerwarteter Fehler: {e}")
        sys.exit(1)
