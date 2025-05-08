# Print Task Crawler

Ein Python-Webcrawler zur Erfassung von Ausschreibungen im Bereich Druck, ausgeführt über GitHub Actions und veröffentlicht via GitHub Pages.

## Funktionen
- Durchsucht 20 Webseiten nach 10 Kriterien (Platzhalter).
- Extrahiert Titel, Datum, Link, Budget, Anschrift.
- Läuft alle 2 Stunden automatisch oder manuell.
- Ergebnisse als CSV und HTML mit interaktiver Tabelle.

## Installation
```bash
pip install -r requirements.txt
```

## Lokaler Start
```bash
python crawler/crawl.py
```

## GitHub Actions
Automatischer Crawl alle 2 Stunden oder manuell via GitHub UI.

## GitHub Pages
Die Datei `docs/index.html` zeigt die Ausschreibungen mit CSV-Export (SSL über GitHub aktiviert).

## Anpassung
URLs & HTML-Parser-Logik in `crawl.py` individuell anpassen.
