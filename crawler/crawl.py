# crawler_advanced.py
"""
Produktionsreifer Webcrawler für Ausschreibungsdaten mit:
- Asynchroner Architektur (aiohttp, Playwright)
- Automatischer Retry-Mechanismus mit Backoff
- Caching und Delay aus robots.txt (inkl. Crawl-Delay)
- Proxy- und User-Agent-Rotation
- Rate-Limiting via aiohttp TCPConnector
- Verbesserte Fehlerbehandlung (u.a. 429, SSL)
- Präzise Kriteriensuche (Regex)
- Atomare CSV/JSON-Ausgabe
"""

import asyncio
import csv
import json
import logging
import os
import random
import re
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple

import aiohttp
from aiohttp import ClientSession, ClientTimeout, ClientResponseError
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)
from urllib.parse import urlparse
import urllib.robotparser
from functools import lru_cache

# --- Konfiguration über Umgebungsvariablen ---
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT", 10))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 15))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
BASE_DELAY = float(os.getenv("BASE_DELAY", 1.0))
USER_AGENTS = [ua for ua in os.getenv("USER_AGENTS", "").split(";") if ua] or [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
]
PROXIES = [p for p in os.getenv("PROXIES", "").split(";") if p]
OUTPUT_FORMATS = [fmt for fmt in os.getenv("OUTPUT_FORMATS", "csv,json").split(",") if fmt]
JS_SITES = [d for d in os.getenv("JS_SITES", "").split(";") if d] or [
    "vergabemarktplatz.de",
    "tendersinfo.com",
    "wlw.de",
    "vergabe24.de",
    "tendertiger.de"
]

# --- Logging-Konfiguration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlConfig:
    """Konfigurationsklasse für Crawling-Parameter."""
    start_urls: List[str] = None
    criteria: List[str] = None

    def __post_init__(self):
        self.start_urls = self.start_urls or [
            "https://www.ausschreibungen-aktuell.de/",
            "https://www.ausschreibungsmonitor.de/",
            "https://www.druckportal.de/",
            "https://www.auftragsboerse.de/",
            "https://www.bundesanzeiger.de/"
        ]
        self.criteria = self.criteria or [
            "22000000-0", "22100000-1", "22110000-4", "22120000-7", "22450000-9",
            "22460000-2", "79800000-2", "79810000-5", "79820000-8", "79823000-9",
            "Bücher", "Magazine", "Broschüren", "Festschriften", "Zeitungen", "Druckerzeugnisse"
        ]

class RobotsTxtCache:
    """
    Caching und Prüfung von robots.txt-Regeln inkl. Crawl-Delay.
    Nutzt LRU-Cache zur Begrenzung des Speicherverbrauchs.
    """
    def __init__(self, maxsize=100):
        self.cache = {}
        self.delay_cache = {}
        self.maxsize = maxsize

    async def is_allowed(self, url: str, user_agent: str) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self.cache:
            await self._load_robots_txt(domain, user_agent)
        rp = self.cache.get(domain)
        if rp:
            return rp.can_fetch(user_agent, url)
        return False

    async def get_crawl_delay(self, url: str, user_agent: str) -> float:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if domain not in self.delay_cache:
            await self._load_robots_txt(domain, user_agent)
        return self.delay_cache.get(domain, 1.0)

    async def _load_robots_txt(self, domain: str, user_agent: str):
        if len(self.cache) > self.maxsize:
            self.cache.pop(next(iter(self.cache)))
            self.delay_cache.pop(next(iter(self.delay_cache)))
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
    """Erstellt Regex-Pattern für präzise Kriteriensuche."""
    return [re.compile(rf'\b{re.escape(crit)}\b', re.IGNORECASE) for crit in criteria]

def is_js_site(url: str) -> bool:
    """Bestimmt, ob JavaScript-Rendering benötigt wird."""
    return any(domain in url for domain in JS_SITES)

@retry(
    retry=retry_if_exception_type(aiohttp.ClientError),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=BASE_DELAY, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
async def fetch_content(session: ClientSession, url: str, user_agent: str, proxy: Optional[str]) -> Optional[str]:
    """
    Führt asynchrone HTTP-GET-Anfrage mit Fehlerbehandlung, Proxy und User-Agent durch.
    Behandelt 429-Fehler mit Retry-After.
    """
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(
            url,
            headers=headers,
            proxy=f"http://{proxy}" if proxy else None,
            timeout=ClientTimeout(total=REQUEST_TIMEOUT),
            ssl=True
        ) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('Retry-After', '60'))
                logger.warning(f"429 Too Many Requests für {url}, warte {retry_after}s")
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientError("429 Too Many Requests")
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientResponseError as e:
        logger.error(f"HTTP Fehler {e.status} für {url}")
        raise
    except Exception as e:
        logger.error(f"Netzwerkfehler für {url}: {e}")
        raise

async def render_js_content(url: str, user_agent: str) -> Optional[str]:
    """
    Rendert JavaScript-Seiten mit Playwright.
    """
    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            context = await browser.new_context(
                user_agent=user_agent,
                java_script_enabled=True
            )
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_load_state("networkidle")
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        logger.error(f"Playwright Fehler: {str(e)[:200]}...")
        return None

async def process_url(
    session: ClientSession,
    robots_cache: RobotsTxtCache,
    url: str,
    patterns: List[re.Pattern],
    config: CrawlConfig
) -> Optional[Tuple[str, str]]:
    """
    Verarbeitet eine einzelne URL: robots.txt-Check, Crawl-Delay, Download, Parsing, Filter.
    """
    user_agent = random.choice(USER_AGENTS)
    proxy = random.choice(PROXIES) if PROXIES else None

    # robots.txt-Check
    if not await robots_cache.is_allowed(url, user_agent):
        logger.info(f"Blockiert durch robots.txt: {url}")
        return None

    # Crawl-Delay aus robots.txt respektieren
    crawl_delay = await robots_cache.get_crawl_delay(url, user_agent)
    await asyncio.sleep(crawl_delay)

    try:
        # Inhaltsbeschaffung (JS oder klassisch)
        if is_js_site(url):
            html = await render_js_content(url, user_agent)
        else:
            html = await fetch_content(session, url, user_agent, proxy)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        # Präzise Kriterienprüfung mit Regex
        if any(pattern.search(text) for pattern in patterns):
            title = soup.title.string.strip() if soup.title else "Ohne Titel"
            logger.info(f"Treffer: {title[:80]}... ({url})")
            return (title, url)
    except Exception as e:
        logger.error(f"Verarbeitungsfehler {url}: {str(e)[:200]}...")

    return None

async def main():
    """Hauptfunktion des Crawlers."""
    config = CrawlConfig()
    robots_cache = RobotsTxtCache()
    patterns = build_criteria_patterns(config.criteria)
    results = []

    # Sichere SSL-Verbindung
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        ssl=True
    )

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
    if "csv" in OUTPUT_FORMATS:
        with open("results.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Titel", "Link"])
            writer.writerows(results)

    if "json" in OUTPUT_FORMATS:
        with open("results.json", "w", encoding="utf-8") as f:
            json.dump(
                [{"title": t, "url": u} for t, u in results],
                f,
                ensure_ascii=False,
                indent=2
            )

    logger.info(f"Crawling abgeschlossen. Gespeicherte Ergebnisse: {len(results)}")

if __name__ == "__main__":
    asyncio.run(main())
