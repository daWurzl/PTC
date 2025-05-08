import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

# Platzhalter-URLs
URLS = [f"https://webseite-{i}.de" for i in range(1, 21)]

# Platzhalter-Kriterien
CRITERIA = [f"Kriterium-{i}" for i in range(1, 11)]

results = []

for url in URLS:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen von {url}: {e}")
        continue

    soup = BeautifulSoup(response.text, 'html.parser')
    entries = soup.find_all('article')
    for entry in entries:
        text = entry.get_text()
        if all(k in text for k in CRITERIA):
            results.append({
                "Titel": entry.find('h2').get_text(strip=True) if entry.find('h2') else "Unbekannt",
                "Datum": datetime.now().strftime('%Y-%m-%d'),
                "Link": url,
                "Budget": "k.A.",
                "Anschrift": "Platzhalter-Adresse"
            })

with open('data/results.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["Titel", "Datum", "Link", "Budget", "Anschrift"])
    writer.writeheader()
    writer.writerows(results)
