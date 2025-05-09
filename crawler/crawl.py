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
    "quoka.de",
    "dtvp.de",
    "ted.europa.eu",
    "evergabe.de",
    "aumass.de",
    "auftraege.bayern.de",
    "deutsches-ausschreibungsblatt.de",
    "vergabe.nrw.de",
    "subreport.de",
    "markt.de",
    "shpock.com",
    "kalaydo.de",
    "dhd24.com",
    "locanto.de",
    "anibis.ch",
    "facebook.com/marketplace"
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
    "PC", "Auto",  # Neue Kriterien für Kleinanzeigen
    "22000000-0", "22100000-1", "22110000-4", "22120000-7", "22450000-9",
    "22460000-2", "79800000-2", "79810000-5", "79820000-8", "79823000-9",
    "Bücher", "Magazine", "Broschüren", "Festschriften", "Zeitungen", "Druckerzeugnisse", "Kopien",
    "Briefpapier", "Umschläge", "Briefhüllen", "Geschäftspapier", "Visitenkarten",
    "Geschäftsberichte", "Mappen", "Schreibhefte",
    "Flyer", "Prospekte", "Plakate", "Werbetafeln", "Werbedisplays",
    "Aufsteller", "Displays",
    "Tickets", "Eintrittskarten", "Einladungskarten", "Eventdrucksachen", "Posterkarten",
    "Verpackungen", "Geschenkverpackungen", "Verpackungsbanderolen", "Tragetaschen",
    "Klebefolien", "Stickersets",
    "Hochzeitskarten", "Trauerdrucksachen", "Sterbebilder", "Fahnen", "Flaggen",
    "Personalisierte Drucksachen", "Plastikkarten", "Geschenkpapier",
    "Formulare", "Vordrucke", "Bedienungsanleitungen", "Wartungshandbücher",
    "Vereinsdrucksachen", "Mitgliedsausweise", "Dokumentationen",
    "Recyclingverpackungen", "FSC-Zertifikat", "3D-Drucke", "Geprägte Druckmaterialien",
    "Wasserfeste Druckerzeugnisse", "Umweltfreundliche Druckerzeugnisse",
    "Banner", "Wandkalender", "Kalender", "Kataloge", "Speisekarten",
    "Gutscheine", "Urkunden", "Großformatdrucke", "Schilder",
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

def is_js_site(url):
    url = url.lower()
    return any(js_site in url for js_site in JS_SITES)

def get_page_text(url):
    user_agent = random.choice(USER_AGENTS)
    headers = {"User-Agent": user_agent}
    time.sleep(random.uniform(2, 6))

    print(f"\nLade: {url}")
    if is_js_site(url):
        print("-> Versuche Playwright (JavaScript-Seite)")
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(user_agent=user_agent)
                page = context.new_page()
                page.goto(url, timeout=30000)
                time.sleep(random.uniform(0.5, 2))
                content = page.content()
                browser.close()
                if len(content) < 500:
                    print("⚠️ Playwright: Sehr wenig Inhalt geladen!")
                else:
                    print(f"Playwright: {len(content)} Zeichen geladen.")
                print("HTML-Ausschnitt:", content[:300].replace('\n', ' '))
                return content
        except Exception as e:
            print(f"⚠️ Fehler mit Playwright bei {url}: {e}")
            return ""
    else:
        print("-> Versuche Requests")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            text = response.text
            if len(text) < 500:
                print("⚠️ Requests: Sehr wenig Inhalt geladen!")
            else:
                print(f"Requests: {len(text)} Zeichen geladen.")
            print("HTML-Ausschnitt:", text[:300].replace('\n', ' '))
            return text
        except Exception as e:
            print(f"⚠️ Fehler mit Requests bei {url}: {e}")
            return ""

def page_matches_criteria(text, criteria):
    for criterion in criteria:
        if criterion.lower() in text.lower():
            print(f"Treffer für Suchbegriff: '{criterion}'")
            return True
    return False

def crawl():
    results = []
    for url in URLS:
        html = get_page_text(url)
        if not html:
            print(f"Keine Daten von {url} erhalten.")
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        print(f"Text-Länge nach Parsing: {len(text)} Zeichen")

        if page_matches_criteria(text, CRITERIA):
            title = soup.title.string.strip() if soup.title else "Kein Titel"
            date = datetime.now().strftime("%Y-%m-%d")
            results.append([title, date, url, "k.A.", "n.v."])
            print(f"-> Ergebnis hinzugefügt: {title}")
        else:
            print("-> Keine Suchbegriffe gefunden.")

    # Ergebnisse speichern
    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Titel", "Datum", "Link", "Budget", "Anschrift"])
        writer.writerows(results)

    print(f"\n{len(results)} Treffer gespeichert in {OUTPUT_FILE}")
    if results:
        print("Ergebnisse:")
        for r in results:
            print(r)

if __name__ == "__main__":
    crawl()
