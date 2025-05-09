import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import random
import time

# 1. JavaScript-basierte Seiten (werden mit Playwright geladen)
JS_SITES = [
    "facebook.com/marketplace",
    "kleinanzeigen.de",
    "quoka.de"
]

# 2. Ausschreibungs-Webseiten (bereinigt und ergänzt)
URLS = [
    "https://ausschreibungen-deutschland.de/cpv/",
    "https://www.dtvp.de/Center/common/project/search.do?method=showExtendedSearch&fromExternal=true",
    "https://ted.europa.eu/de/",
    "https://ted.europa.eu/de/advanced-search",
    "https://ted.europa.eu/de/search/result?FT=Druckereidienste+und+verbundene+Dienstleistungen+des+Druckgewerbes",
    "https://www.evergabe.de/",
    "https://www.aumass.de/ausschreibungen/bayern",
    "https://www.auftraege.bayern.de/Dashboards/Dashboard_off?BL=09",
    "https://www.deutsches-ausschreibungsblatt.de/auftrag-finden#/",
    "https://www.vergabe.nrw.de/",
    "https://ausschreibungen-deutschland.de/",
    "https://subreport.de/ausschreibungen/auftraege-suchen/",
    "https://www.kleinanzeigen.de",
    "https://www.quoka.de",
    "https://www.markt.de",
    "https://www.shpock.com",
    "https://www.kalaydo.de",
    "https://www.dhd24.com",
    "https://www.locanto.de",
    "https://www.anibis.ch",
    "https://www.facebook.com/marketplace/"
]

# 3. Suchkriterien aus CPV-Codes und Themenbereichen
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
    # Personalisierte & Hochwertige Drucksachen
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

# 4. Ergebnisdatei
OUTPUT_FILE = "results.csv"

# Liste realistischer User-Agents (kann beliebig erweitert werden)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
]

# Prüft, ob eine URL JavaScript erfordert
def is_js_site(url):
    return any(js in url for js in JS_SITES)

# Liefert HTML-Quelltext mit requests oder Playwright
def get_page_text(url):
    # Zufälligen User-Agent wählen
    user_agent = random.choice(USER_AGENTS)
    headers = {"User-Agent": user_agent}

    # Zufällige Wartezeit (zwischen 2 und 6 Sekunden) vor jedem Request, um menschliches Verhalten zu simulieren
    time.sleep(random.uniform(2, 6))

    if is_js_site(url):
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                # Headless-Browser starten (headless=True)
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(user_agent=user_agent)
                page = context.new_page()
                page.goto(url, timeout=20000)

                # Nach dem Laden der Seite noch eine kurze, zufällige Wartezeit (0.5-2s)
                time.sleep(random.uniform(0.5, 2))

                content = page.content()
                browser.close()
                return content
        except Exception as e:
            print(f"⚠️ Fehler mit Playwright bei {url}: {e}")
            return ""
    else:
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"⚠️ Fehler mit Requests bei {url}: {e}")
            return ""

# Text auf Suchbegriffe prüfen
def page_matches_criteria(text, criteria):
    return any(criterion.lower() in text.lower() for criterion in criteria)

# Hauptcrawler
def crawl():
    results = []

    for url in URLS:
        html = get_page_text(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        if page_matches_criteria(text, CRITERIA):
            title = soup.title.string.strip() if soup.title else "Kein Titel"
            date = datetime.now().strftime("%Y-%m-%d")
            results.append([title, date, url, "k.A.", "n.v."])

    # Ergebnisse speichern
    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Titel", "Datum", "Link", "Budget", "Anschrift"])
        writer.writerows(results)

    print(f"{len(results)} Treffer gespeichert in {OUTPUT_FILE}")

if __name__ == "__main__":
    crawl()
