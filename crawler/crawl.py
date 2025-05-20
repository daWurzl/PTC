import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
from urllib.parse import urlparse

# ---------------------------------------------------------------
# Ressourcenlimits für GitHub Actions optimiert
# ---------------------------------------------------------------
if os.name == 'posix':
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (300, 300))
    resource.setrlimit(resource.RLIMIT_AS, (200 * 1024 * 1024, 200 * 1024 * 1024))  # 200 MB
    resource.setrlimit(resource.RLIMIT_NOFILE, (50, 50))
    resource.setrlimit(resource.RLIMIT_NPROC, (10, 10))

# ---------------------------------------------------------------
# Asyncio-Konfiguration mit uvloop für bessere Performance
# ---------------------------------------------------------------
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator, ConfigDict
from playwright.async_api import async_playwright

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
        if not self._browser or self._browser.is_connected():
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=["--single-process", "--disable-dev-shm-usage"]
            )
        return self._browser
    
    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

# ---------------------------------------------------------------
# Konfiguration und Hauptlogik
# ---------------------------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseModel):
    START_URLS: List[str] = Field(default=[...])
    USER_AGENTS: List[str] = Field(default=[...])
    OUTPUT_CSV: str = Field(default="data/results.csv")
    CONCURRENT_REQUESTS: int = Field(default=2)  # Stark reduziert
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
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def fetch_js_page(url: str, user_agent: str, proxy: Optional[str] = None) -> Optional[str]:
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
    # Unveränderte Verarbeitungslogik...
    
async def crawl():
    connector = TCPConnector(
        limit=settings.CONCURRENT_REQUESTS,
        limit_per_host=1,  # Max 1 Verbindung pro Host
        force_close=True
    )
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Unveränderte Crawling-Logik...
    
    # Browser-Pool bereinigen
    await BrowserPool().close()

if __name__ == "__main__":
    try:
        asyncio.run(crawl())
    except Exception as e:
        logger.error(f"Kritischer Fehler: {e}")
        sys.exit(1)
