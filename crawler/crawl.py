import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
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
from pydantic import BaseModel, Field, field_validator, ConfigDict
from playwright.async_api import async_playwright

# --- Konfiguration via Pydantic ---
class Settings(BaseModel):
    START_URLS: List[str] = Field(
        default=["https://www.example.com/"],
        description="Kommagetrennte Liste von Start-URLs"
    )
    USER_AGENTS: List[str] = Field(
        default=[
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ],
        description="Kommagetrennte Liste von User-Agents"
    )
    OUTPUT_CSV: str = Field(
        default="data/results.csv",
        description="Ausgabepfad für CSV-Datei"
    )
    CONCURRENT_REQUESTS: int = Field(
        default=5,
        description="Maximale parallele Anfragen"
    )
    REQUEST_TIMEOUT: int = Field(
        default=30,  # Erhöht für JS-Rendering
        description="Timeout in Sekunden pro Request"
    )
    JS_SITES: List[str] = Field(
        default=[],
        description="Domains mit JavaScript-Rendering (kommagetrennt)"
    )
    USE_PROXIES: bool = Field(
        default=False,
        description="Proxy-Handling aktivieren"
    )
    PROXIES: List[str] = Field(
        default=[],
        description="Liste der Proxy-URLs (z.B. http://user:pass@host:port)"
    )

    model_config = ConfigDict(env_file=".env", env_file_encoding="utf-8")

    @field_validator("START_URLS", "USER_AGENTS", "JS_SITES", "PROXIES", mode="before")
    @classmethod
    def split_comma_separated(cls, v):
        """Teilt kommagetrennte Strings in Listen auf."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

settings = Settings()

# --- Logging-Konfiguration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# --- Fehlerzähler ---
error_count = 0

# Semaphore für parallele Playwright-Instanzen (z.B. max. 3 gleichzeitig)
playwright_semaphore = asyncio.Semaphore(3)

async def fetch_page(session: aiohttp.ClientSession, url: str, user_agent: str, **kwargs) -> Optional[str]:
    """Lädt HTML-Inhalt asynchron mit Timeout und User-Agent (ohne JS)."""
    global error_count
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(
            url,
            headers=headers,
            timeout=ClientTimeout(total=settings.REQUEST_TIMEOUT),
            **kwargs
        ) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as e:
        error_count += 1
        logger.warning(f"Fehler beim Abruf von {url}: {str(e)[:50]}...")
        return None

async def fetch_js_page(url: str, user_agent: str, proxy: Optional[str] = None) -> Optional[str]:
    """Lädt JavaScript-seitig gerenderten Inhalt mit Playwright (Headless-Browser)."""
    global error_count
    async with playwright_semaphore:  # Begrenze parallele Browser-Instanzen
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=user_agent,
                    proxy={"server": proxy} if proxy else None,
                    ignore_https_errors=True
                )
                page = await context.new_page()
                await page.goto(url, timeout=settings.REQUEST_TIMEOUT * 1000)
                content = await page.content()
                await context.close()
                await browser.close()
                return content
        except Exception as e:
            error_count += 1
            logger.warning(f"Playwright-Fehler bei {url}: {str(e)[:50]}...")
            return None

async def process_url(
    session: aiohttp.ClientSession,
    url: str
) -> Optional[Tuple[str, str]]:
    """
    Verarbeitet eine URL:
    - Erkennt, ob JS-Rendering nötig ist
    - Lädt HTML (mit oder ohne JS)
    - Parst Title und liefert (title, url)
    """
    global error_count

    # Prüfe, ob Domain JS-Rendering benötigt
    domain = urlparse(url).netloc
    use_js = any(js_domain in domain for js_domain in settings.JS_SITES)

    # Proxy-Handling
    proxy = None
    if settings.USE_PROXIES and settings.PROXIES:
        proxy = random.choice(settings.PROXIES)
        logger.debug(f"Verwende Proxy: {proxy}")

    user_agent = random.choice(settings.USER_AGENTS)

    try:
        if use_js:
            logger.info(f"Verwende Playwright für JS-Seite: {url}")
            html = await fetch_js_page(url, user_agent, proxy)
        else:
            if proxy:
                html = await fetch_page(session, url, user_agent, proxy=proxy)
            else:
                html = await fetch_page(session, url, user_agent)

        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        title = title_tag.string.strip() if title_tag else "Ohne Titel"
        return title, url

    except Exception as e:
        error_count += 1
        logger.warning(f"Verarbeitungsfehler bei {url}: {str(e)[:50]}...")
        return None

async def crawl():
    """
    Hauptfunktion:
    - Crawlt alle Start-URLs
    - Nutzt parallele Requests
    - Speichert Ergebnisse in CSV
    """
    # Verzeichnis für die Ausgabe prüfen
    out_dir = os.path.dirname(settings.OUTPUT_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    logger.info(
        f"Starte Crawling mit {len(settings.START_URLS)} URLs, "
        f"Ausgabe: {settings.OUTPUT_CSV}"
    )

    results: List[Tuple[str, str]] = []

    connector = aiohttp.TCPConnector(limit=settings.CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_url(session, url) for url in settings.START_URLS]
        for future in asyncio.as_completed(tasks):
            res = await future
            if res:
                results.append(res)
                logger.info(f"Gefunden: {res[0][:30]}... ({res[1]})")

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
