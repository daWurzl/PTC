import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
from urllib.parse import urlparse

# Korrekter Import des gesamten aiohttp-Moduls
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator, ConfigDict
from playwright.async_api import async_playwright

import psutil  # <--- NEU: Für Ressourcen-Logging

# ---------------------------------------------------------------
# Ressourcenlimits für GitHub Actions optimiert
# ---------------------------------------------------------------
if os.name == 'posix':
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (600, 600))
    resource.setrlimit(resource.RLIMIT_AS, (1024 * 1024 * 1024, 1024 * 1024 * 1024))  # 1 GB
    resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))
    resource.setrlimit(resource.RLIMIT_NPROC, (128, 128))

# ---------------------------------------------------------------
# Asyncio-Konfiguration mit uvloop für bessere Performance
# ---------------------------------------------------------------
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ---------------------------------------------------------------
# Globaler Playwright Browser Pool
# ---------------------------------------------------------------
class BrowserPool:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._playwright = None
            cls._instance._browser = None
        return cls._instance

    async def get_browser(self):
        if not self._browser or not self._browser.is_connected():
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=["--single-process", "--disable-dev-shm-usage", "--no-sandbox"]
            )
        return self._browser

    async def close(self):
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

# ---------------------------------------------------------------
# Konfiguration und Hauptlogik
# ---------------------------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseModel):
    START_URLS: List[str] = Field(default=[
        "https://www.bund.de/",
        "https://www.bundestag.de/",
        "https://www.euractiv.de/",
        "https://www.dw.com/de/",
        "https://www.zdf.de/nachrichten/",
        "https://www.spiegel.de/",
        "https://www.faz.net/",
        "https://www.welt.de/",
        "https://www.handelsblatt.com/",
        "https://www.zeit.de/"
    ])
    USER_AGENTS: List[str] = Field(default=[
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-S926B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
        "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (X11; CrOS x86_64 15633.69.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ])
    OUTPUT_CSV: str = Field(default="data/results.csv")
    CONCURRENT_REQUESTS: int = Field(default=1)  # Stark reduziert
    REQUEST_TIMEOUT: int = Field(default=30)
    JS_SITES: List[str] = Field(default=[])
    USE_PROXIES: bool = Field(default=False)
    PROXIES: List[str] = Field(default=[
        "http://138.199.233.152:80",
        "http://89.58.57.45:80",
        "http://161.35.70.249:8080",
        "http://178.63.237.156:80",
        "http://217.112.96.0:8080",
        "http://213.169.33.7:3128",
        "http://77.242.21.133:8080",
        "http://159.65.125.194:80",
        "http://188.68.52.244:80",
        "http://91.211.212.6:8080"
    ])

    model_config = ConfigDict(env_file=env_path, env_file_encoding="utf-8")

    @field_validator("START_URLS", "USER_AGENTS", "JS_SITES", "PROXIES", mode="before")
    @classmethod
    def split_comma_separated(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

settings = Settings()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

error_count = 0

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
    browser_pool = BrowserPool()
    browser = await browser_pool.get_browser()
    try:
        context = await browser.new_context(
            user_agent=user_agent,
            proxy={"server": proxy} if proxy else None,
            ignore_https_errors=True
        )
        async with context:
            page = await context.new_page()
            await page.goto(url, timeout=settings.REQUEST_TIMEOUT * 1000)
            return await page.content()
    except Exception as e:
        logger.warning(f"Playwright-Fehler: {str(e)[:50]}...")
        return None

async def process_url(session: aiohttp.ClientSession, url: str) -> Optional[Tuple[str, str]]:
    global error_count
    domain = urlparse(url).netloc
    use_js = any(js_domain in domain for js_domain in settings.JS_SITES)

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
    connector = TCPConnector(
        limit=settings.CONCURRENT_REQUESTS,
        limit_per_host=1,  # Max 1 Verbindung pro Host
        force_close=True
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        # Crawling-Logik
        out_dir = os.path.dirname(settings.OUTPUT_CSV)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        logger.info(f"Starte Crawling mit {len(settings.START_URLS)} URLs, Ausgabe: {settings.OUTPUT_CSV}")

        # Ressourcen-Logging vor dem Crawl
        logger.info(f"RAM-Auslastung: {psutil.virtual_memory().percent}% | Prozesse: {len(psutil.pids())}")

        results = []
        tasks = [process_url(session, url) for url in settings.START_URLS]

        for future in asyncio.as_completed(tasks):
            res = await future
            if res:
                results.append(res)
                logger.info(f"Gefunden: {res[0][:30]}... ({res[1]})")
            # Ressourcen-Logging nach jedem Request
            logger.info(f"RAM-Auslastung: {psutil.virtual_memory().percent}% | Prozesse: {len(psutil.pids())}")

        # CSV schreiben
        with open(settings.OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Title", "URL"])
            writer.writerows(results)

        logger.info(f"Crawling abgeschlossen: {len(results)} Einträge, {error_count} Fehler")

    # Browser-Pool bereinigen
    await BrowserPool().close()

if __name__ == "__main__":
    try:
        asyncio.run(crawl())
    except Exception as e:
        logger.error(f"Kritischer Fehler: {e}")
        sys.exit(1)
