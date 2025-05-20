import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
from urllib.parse import urlparse

# ---------------------------------------------------------------
# Ressourcenlimits (nur auf Unix/Linux-Systemen aktiv)
# ---------------------------------------------------------------
if os.name == 'posix':
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (300, 300))
    memory_limit = 500 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator, ConfigDict
from playwright.async_api import async_playwright

# ---------------------------------------------------------------
# Konfiguration mit dynamischem .env-Pfad
# ---------------------------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseModel):
    START_URLS: List[str] = Field(
        default=[
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
        ],
        description="10 Fallback-Start-URLs für deutsche/europäische Seiten"
    )
    
    USER_AGENTS: List[str] = Field(
        default=[
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
        ],
        description="10 aktuelle User-Agents für verschiedene Plattformen"
    )

    OUTPUT_CSV: str = Field(default="data/results.csv")
    CONCURRENT_REQUESTS: int = Field(default=5)
    REQUEST_TIMEOUT: int = Field(default=30)
    JS_SITES: List[str] = Field(default=[])
    USE_PROXIES: bool = Field(default=False)
    PROXIES: List[str] = Field(default=[])

    model_config = ConfigDict(env_file=env_path, env_file_encoding="utf-8")

    @field_validator("START_URLS", "USER_AGENTS", "JS_SITES", "PROXIES", mode="before")
    @classmethod
    def split_comma_separated(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

settings = Settings()

# ---------------------------------------------------------------
# Logging-Konfiguration
# ---------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------
# Globale Variablen
# ---------------------------------------------------------------
error_count = 0
playwright_semaphore = asyncio.Semaphore(3)

# ---------------------------------------------------------------
# Hauptfunktionen (unverändert)
# ---------------------------------------------------------------
async def fetch_page(session: aiohttp.ClientSession, url: str, user_agent: str, **kwargs) -> Optional[str]:
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
    global error_count
    async with playwright_semaphore:
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

async def process_url(session: aiohttp.ClientSession, url: str) -> Optional[Tuple[str, str]]:
    global error_count
    domain = urlparse(url).netloc
    use_js = any(js_domain in domain for js_domain in settings.JS_SITES)

    proxy = random.choice(settings.PROXIES) if settings.USE_PROXIES and settings.PROXIES else None
    user_agent = random.choice(settings.USER_AGENTS)

    try:
        html = await (fetch_js_page(url, user_agent, proxy) if use_js 
                     else fetch_page(session, url, user_agent, proxy=proxy))
        
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
    out_dir = os.path.dirname(settings.OUTPUT_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    logger.info(f"Starte Crawling mit {len(settings.START_URLS)} URLs")

    results = []
    connector = aiohttp.TCPConnector(limit=settings.CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_url(session, url) for url in settings.START_URLS]
        for future in asyncio.as_completed(tasks):
            if res := await future:
                results.append(res)
                logger.info(f"Gefunden: {res[0][:30]}... ({res[1]})")

    with open(settings.OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "URL"])
        writer.writerows(results)

    logger.info(f"Crawling abgeschlossen: {len(results)} Einträge, {error_count} Fehler")

if __name__ == "__main__":
    try:
        asyncio.run(crawl())
    except Exception as e:
        logger.error(f"Kritischer Fehler: {e}")
        sys.exit(1)
