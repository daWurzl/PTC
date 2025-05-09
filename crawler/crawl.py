from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import csv
import time

SEARCH_TERMS = ["PC", "Auto"]
OUTPUT_FILE = "results.csv"

def scrape_kleinanzeigen(search_term):
    url = f"https://www.kleinanzeigen.de/s-suchanfrage.html?keywords={search_term}"
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        time.sleep(3)  # Warten, bis JS geladen hat

        # Optional: Scrollen, um mehr Ergebnisse zu laden
        for _ in range(3):
            page.mouse.wheel(0, 10000)
            time.sleep(2)

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    for offer in soup.select("article.aditem"):
        title = offer.select_one(".text-module-begin a")
        price = offer.select_one(".aditem-main--middle--price-shipping .aditem-main--middle--price")
        if title:
            results.append({
                "Titel": title.get_text(strip=True),
                "Link": "https://www.kleinanzeigen.de" + title['href'],
                "Preis": price.get_text(strip=True) if price else "k.A.",
                "Suchbegriff": search_term
            })
    return results

def main():
    all_results = []
    for term in SEARCH_TERMS:
        print(f"Suche nach: {term}")
        res = scrape_kleinanzeigen(term)
        print(f"{len(res)} Treffer für '{term}' gefunden.")
        all_results.extend(res)

    # Ergebnisse speichern
    with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Titel", "Link", "Preis", "Suchbegriff"])
        writer.writeheader()
        writer.writerows(all_results)
    print(f"Gespeichert: {len(all_results)} Ergebnisse in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
