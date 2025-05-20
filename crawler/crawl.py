import os
import sys
import asyncio
import csv
import logging
import random
from typing import List, Optional, Tuple
from urllib.parse import urlparse

# ---------------------------------------------------------------
# TESTMODUS-KONFIGURATION (Kann später entfernt werden)
# ---------------------------------------------------------------
TEST_MODE = os.getenv('TEST_MODE', '0') == '1'  # Über Umgebungsvariable aktivieren
TEST_URL = "https://mein-teekontor.de/"

# ---------------------------------------------------------------
# Basis-Imports
# ---------------------------------------------------------------
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator, ConfigDict
from playwright.async_api import async_playwright
import psutil

# ---------------------------------------------------------------
# Ressourcenlimits für Stabilität
# ---------------------------------------------------------------
if os.name == 'posix':
    import resource
    resource.setrlimit(resource.RLIMIT_CPU, (600, 600))
    resource.setrlimit(resource.RLIMIT_AS, (1024 * 1024 * 1024, 1024 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (1024, 1024))
    resource.setrlimit(resource.RLIMIT_NPROC, (128, 128))

# ---------------------------------------------------------------
# Asyncio-Optimierungen
# ---------------------------------------------------------------
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ---------------------------------------------------------------
# Playwright Browser Pool
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
        if self._playwright:
            await self._playwright.stop()

# ---------------------------------------------------------------
# Hauptkonfiguration
# ---------------------------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), ".env")

class Settings(BaseModel):
    START_URLS: List[str] = Field(default=[TEST_URL] if TEST_MODE else [
        "https://www.bund.de/", 
        "https://www.bundestag.de/",
        # ... andere URLs ...
    ])
    
    # ... restliche Konfiguration unverändert ...

    model_config = ConfigDict(env_file=env_path, env_file_encoding="utf-8")

settings = Settings()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------
# TESTMODUS-LOGIK (Kann später entfernt werden)
# ---------------------------------------------------------------
async def test_h2_extractor(session: aiohttp.ClientSession, url: str) -> Optional[Tuple[str, str]]:
    """Extrahiert h2-Links für Test-URL"""
    try:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            for h2 in soup.find_all('h2'):
                a_tag = h2.find('a')
                if a_tag and a_tag.has_attr('href'):
                    return (h2.get_text(strip=True), a_tag['href'])
    except Exception as e:
        logger.error(f"TESTMODUS-Fehler: {e}")
    return None

# ---------------------------------------------------------------
# Hauptfunktionen
# ---------------------------------------------------------------
async def process_url(session: aiohttp.ClientSession, url: str) -> Optional[Tuple[str, str]]:
    if TEST_MODE:
        return await test_h2_extractor(session, url)
    
    # ... ursprüngliche process_url-Logik ...

async def crawl():
    connector = TCPConnector(limit=1) if TEST_MODE else TCPConnector()
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # ... ursprüngliche Crawl-Logik ...

if __name__ == "__main__":
    try:
        asyncio.run(crawl())
    except Exception as e:
        logger.error(f"Fehler: {e}")
        sys.exit(1)
