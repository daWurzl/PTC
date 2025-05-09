import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging-Konfiguration:
# - Logs werden NUR in die Datei "crawler.log" (UTF-8) geschrieben, keine Konsolenausgabe!
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding="utf-8")
    ]
)

# Beispielhafte Proxy-Liste:
PROXIES = [
    "http://162.249.171.248:4092",
    "http://5.8.240.91:4153",
    "http://189.22.234.44:80",
    "http://184.181.217.206:4145",
    "http://64.71.151.20:8888"
]

# Liste realistischer User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
]

# JS_SITES: Domains, die vermutlich JavaScript zum Rendern benötigen.
JS_SITES = [
    "vergabemarktplatz.de",
    "tendersinfo.com",
    "wlw.de",
    "vergabe24.de",
    "tendertiger.de"
]

# URLS: Enthält nun auch viele große Crawler-Zielseiten (wie gewünscht ergänzt)
URLS = [
    "https://www.bundesanzeiger.de/",
    "https://www.ausschreibungsmonitor.de/",
    "https://www.druckportal.de/",
    "https://www.auftragsboerse.de/",
    "https://www.ausschreibungen-aktuell.de/",
    # Ergänzte Webseiten, die von wichtigen Webcrawlern besucht werden:
    "https://www.yandex.com/",
    "https://www.duckduckgo.com/",
    "https://www.apple.com/",
    "https://www.baidu.com/",
    "https://www.facebook.com/",
    "https://www.yahoo.com/",
    "https://commoncrawl.org/",
    "https://www.swiftype.com/",
    "https://www.exalead.com/"
]

# CRITERIA: Liste von Suchbegriffen (z.B. CPV-Codes, Druckerzeugnisse, Werbemittel, etc.).
CRITERIA = [
    # CPV-Codes
    "22000000-0", "22100000-1", "22110000-4", "22120000-7", "22450000-9",
    "22460000-2", "79800000-2", "79810000-5", "79820000-8", "79823000-9",
    # Standard-Druckerzeugnisse
    "Bücher", "Magazine", "Broschüren", "Festschriften", "Zeitungen", "Druckerzeugnisse", "Kopien",
    # Geschäftsdrucksachen
    "Briefpapier", "Umschläge", "Briefhüllen", "Geschäftspapier", "Visitenkarten",
    "Geschäftsberichte", "Mappen", "Schreibhefte",
    # Werbe- & Marketingmaterial
    "Flyer", "Prospekte", "Plakate", "Werbetafeln", "Werbedisplays",
    "Aufsteller", "Displays",
    # Veranstaltungs- & Spezialdrucksachen
    "Tickets", "Eintrittskarten", "Einladungskarten", "Eventdrucksachen", "Posterkarten",
    # Verpackungen & Etiketten
    "Verpackungen", "Geschenkverpackungen", "Verpackungsbanderolen", "Tragetaschen",
    "Klebefolien", "Stickersets",
    # Personalisierte & hochwertige Drucksachen
    "Hochzeitskarten", "Trauerdrucksachen", "Sterbebilder", "Fahnen", "Flaggen",
    "Personalisierte Drucksachen", "Plastikkarten", "Geschenkpapier",
    # Formulare & Dokumente
    "Formulare", "Vordrucke", "Bedienungsanleitungen", "Wartungshandbücher",
    "Vereinsdrucksachen", "Mitgliedsausweise", "Dokumentationen",
    # Nachhaltige & Spezialdrucke
    "Recyclingverpackungen", "FSC-Zertifikat", "3D-Drucke", "Geprägte Druckmaterialien",
    "Wasserfeste Druckerzeugnisse", "Umweltfreundliche Druckerzeugnisse",
    # Großformate & spezielle Anwendungen
    "Banner", "Wandkalender", "Kalender", "Kataloge", "Speisekarten",
    "Gutscheine", "Urkunden", "Großformatdrucke", "Schilder",
    # Suche / Anbieteranfragen
    "Autor sucht", "Kleinautor sucht", "Suche Druckerei", "Suche Verlag",
    "Suche Buchbinderei", "Suche Digitaldruckerei", "Suche Werbemittelhersteller"
]

OUTPUT_FILE = "results.csv"

def is_js_site(url):
    """Prüft, ob eine URL JavaScript-Rendering benötigt."""
    return any(js in url for js in JS_SITES)

def get_requests_with_retry(url, max_retries=3, use_proxy=True):
    """
    Versucht, die Seite mit Requests abzurufen.
    Nutzt zufälligen User-Agent und optional Proxy.
    """
    for attempt in range(max_retries):
        try:
            user_agent = random.choice(USER_AGENTS)
            headers = {"User-Agent": user_agent}
            proxies = None
            if use_proxy:
                proxy = random.choice(PROXIES)
                proxies = {"http": proxy, "https": proxy}
                logging.info(f"Versuche {url} mit Proxy {proxy}")
            else:
                logging.info(f"Versuche {url} ohne Proxy")
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.warning(f"Fehler bei {url} (Versuch {attempt+1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    logging.error(f"Maximale Versuche für {url} erreicht.")
    return ""

def get_playwright_with_retry(url, max_retries=3):
    """
    Versucht, die Seite mit Playwright zu laden, wenn JavaScript-Rendering benötigt wird.
    """
    for attempt in range(max_retries):
        try:
            user_agent = random.choice(USER_AGENTS)
            proxy = random.choice(PROXIES)
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, proxy={"server": proxy})
                context = browser.new_context(user_agent=user_agent)
                page = context.new_page()
                page.goto(url, timeout=20000)
                try:
                    page.wait_for_selector("body", timeout=5000)
                except PlaywrightTimeout:
                    logging.warning(f"Timeout auf body-Selector bei {url}")
                content = page.content()
                browser.close()
                return content
        except Exception as e:
            logging.warning(f"Playwright-Fehler bei {url} (Versuch {attempt+1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)
    logging.error(f"Maximale Playwright-Versuche für {url} erreicht.")
    return ""

def get_page_text(url):
    """
    Wählt die passende Methode aus (Requests oder Playwright) basierend darauf,
    ob die URL als JavaScript-basierte Seite markiert ist.
    """
    time.sleep(random.uniform(2, 6))
    if is_js_site(url):
        return get_playwright_with_retry(url)
    else:
        return get_requests_with_retry(url)

def page_matches_criteria(text, criteria):
    """Überprüft, ob einer der Suchbegriffe im Text vorkommt."""
    return any(criterion.lower() in text.lower() for criterion in criteria)

def crawl_url(url):
    """
    Crawlt eine einzelne URL und gibt ggf. ein Treffer-Array zurück.
    """
    html = get_page_text(url)
    if not html:
        logging.error(f"Keine Daten von {url} erhalten.")
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    if page_matches_criteria(text, CRITERIA):
        title = soup.title.string.strip() if soup.title and soup.title.string else "Kein Titel"
        date = datetime.now().strftime("%Y-%m-%d")
        logging.info(f"Treffer: {title} von {url}")
        return [title, date, url, "k.A.", "n.v."]
    else:
        logging.info(f"Suchbegriffe in {url} nicht gefunden.")
        return None

def crawl():
    """
    Hauptfunktion des Crawlers: Paralleles Crawlen und Speichern der Treffer.
    """
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(crawl_url, url): url for url in URLS}
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                results.append(result)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Titel", "Datum", "Link", "Budget", "Anschrift"])
        writer.writerows(results)
    logging.info(f"{len(results)} Treffer gespeichert in {OUTPUT_FILE}")

if __name__ == "__main__":
    crawl()
