import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

# 1. Ausschreibungs-Webseiten (bereinigt und ergänzt)
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
    "https://subreport.de/ausschreibungen/auftraege-suchen/"
]

# 2. Suchkriterien aus CPV-Codes und Themenbereichen
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

# 3. Ergebnisdatei
OUTPUT_FILE = "results.csv"

def page_matches_criteria(text, criteria):
    return any(criterion.lower() in text.lower() for criterion in criteria)

def crawl():
    results = []

    for url in URLS:
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)

            if page_matches_criteria(text, CRITERIA):
                title = soup.title.string.strip() if soup.title else "Kein Titel"
                date = datetime.now().strftime("%Y-%m-%d")
                results.append([title, date, url, "k.A.", "n.v."])

        except Exception as e:
            print(f"Fehler bei {url}: {e}")

    # Ergebnisse speichern
    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Titel", "Datum", "Link", "Budget", "Anschrift"])
        writer.writerows(results)

    print(f"{len(results)} Treffer gespeichert in {OUTPUT_FILE}")

if __name__ == "__main__":
    crawl()
