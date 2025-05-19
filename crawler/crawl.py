"""
Asynchroner Webcrawler für Ausschreibungsdaten.
- Sucht nach vordefinierten Kriterien auf Startseiten.
- Speichert nur Titel und Link der Treffer in data/results.csv.
- Beachtet robots.txt, User-Agent-Rotation, Fehlerbehandlung.
"""

import asyncio
import csv
import logging
import os
import random
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
import urllib.robotparser

# --- Konfiguration ---
MAX_CONCURRENT_REQUESTS = 10
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
]
OUTPUT_CSV_PATH = os.path.join("data", "results.csv")

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("crawler.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlConfig:
    """Konfiguration für Startseiten und Suchkriterien."""
    start_urls: List[str] = None
    criteria: List[str] = None

    def __post_init__(self):
        self.start_urls = self.start_urls or [
            "https://www.ausschreibungen-aktuell.de/",
            "https://www.ausschreibungsmonitor.de/",
            "https://www.druckportal.de/",
            "https://www.auftragsboerse.de/",
            "https://www.bundesanzeiger.de/",
            "https://www.aumass.de/ausschreibungen?params=druck/",
            "https://www.ibau.de/auftraege-nach-branche/dienstleistungen/druckauftraege-druckdienstleistungen/",
            "https://oeffentlichevergabe.de/ui/de/search/"
        ]
        self.criteria = self.criteria or [
            "22000000-0", "22100000-1", "22110000-4", "22120000-7", "22450000-9",
            "22460000-2", "79800000-2", "79810000-5", "79820000-8", "79823000-9",
            "Bücher", "Magazine", "Broschüren", "Festschriften", "Zeitungen", "Druckerzeugnisse"
        ]

class RobotsTxtCache:
    """Prüft und cached robots.txt-Regeln inkl. Crawl-Delay."""
    def __init__(self):
        self.cache = {}
        self.delay_cache = {}

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self.cache:
            await self._load_robots_txt(domain, user_agent)
        rp = self.cache.get(domain)
        return rp.can_fetch(user_agent, url) if rp else False

    async def get_crawl_delay(self, url: str, user_agent: str) -> float:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self.delay_cache:
            await self._load_robots_txt(domain, user_agent)
        return self.delay_cache.get(domain, 1.0)

    async def _load_robots_txt(self, domain: str, user_agent: str):
        rp = urllib.robotparser.RobotFileParser()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{domain}/robots.txt", timeout=10, ssl=True) as resp:
                    if resp.status == 200:
                        content = await resp.text()
                        rp.parse(content.splitlines())
            self.cache[domain] = rp
            delay = rp.crawl_delay(user_agent)
            self.delay_cache[domain] = float(delay) if delay else 1.0
        except Exception as e:
            logger.warning(f"Robots.txt Fehler für {domain}: {e}")
            self.cache[domain] = rp
            self.delay_cache[domain] = 1.0

def build_criteria_patterns(criteria: List[str]):
    """Erstellt Regex-Pattern für die Kriteriensuche."""
    return [re.compile(re.escape(crit), re.IGNORECASE) for crit in criteria]

async def fetch_content(session: ClientSession, url: str, user_agent: str) -> Optional[str]:
    """Lädt HTML-Inhalt asynchron mit User-Agent."""
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(
            url,
            headers=headers,
            timeout=ClientTimeout(total=REQUEST_TIMEOUT),
            ssl=True
        ) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Fehler beim Abruf von {url}: {e}")
        return None

async def process_url(
    session: ClientSession,
    robots_cache: RobotsTxtCache,
    url: str,
    patterns: List[re.Pattern],
    config: CrawlConfig
) -> Optional[Tuple[str, str]]:
    """Verarbeitet eine URL: robots.txt-Check, Delay, Download, Kriteriensuche."""
    user_agent = random.choice(USER_AGENTS)

    # robots.txt-Check
    if not await robots_cache.is_allowed(url, user_agent):
        logger.info(f"Blockiert durch robots.txt: {url}")
        return None

    # Crawl-Delay aus robots.txt respektieren
    crawl_delay = await robots_cache.get_crawl_delay(url, user_agent)
    await asyncio.sleep(crawl_delay)

    # HTML laden
    html = await fetch_content(session, url, user_agent)
    if not html:
        return None

    # Inhalt durchsuchen
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    # Treffer prüfen
    if any(pattern.search(text) for pattern in patterns):
        title = soup.title.string.strip() if soup.title else "Ohne Titel"
        logger.info(f"Treffer: {title[:80]}... ({url})")
        return (title, url)
    return None

async def main():
    """Hauptfunktion: Crawlt alle Startseiten und speichert Ergebnisse."""
    config = CrawlConfig()
    robots_cache = RobotsTxtCache()
    patterns = build_criteria_patterns(config.criteria)
    results = []

    os.makedirs("data", exist_ok=True)  # Sicherstellen, dass data/-Ordner existiert

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=True)
    async with ClientSession(connector=connector) as session:
        tasks = [
            process_url(session, robots_cache, url, patterns, config)
            for url in config.start_urls
        ]
        for future in asyncio.as_completed(tasks):
            result = await future
            if result:
                results.append(result)
                logger.debug(f"Aktuelle Treffer: {len(results)}")

    # Ergebnisse speichern
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Titel", "Link"])
        writer.writerows(results)
    logger.info(f"Crawling abgeschlossen. Gespeicherte Ergebnisse: {len(results)}")

if __name__ == "__main__":
    asyncio.run(main())
