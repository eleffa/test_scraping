#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper générique pour une colonne d'URLs dans un fichier Excel.
- Lit l'Excel fourni
- Visite chaque lien (en respectant robots.txt)
- Extrait quelques champs génériques
- Écrit un CSV (1 ligne par lien)
Reprise possible : si un CSV de sortie existe déjà, les URLs déjà traitées sont sautées.
"""

import sys
import time
import csv
import re
import argparse
from pathlib import Path
from urllib.parse import urlparse
from urllib import robotparser

import requests
from requests.exceptions import RequestException, Timeout
from bs4 import BeautifulSoup
import pandas as pd

DEFAULT_INFILE = "liens dossier formation.xlsx"
DEFAULT_SHEET = 0  # index ou nom de feuille
DEFAULT_URL_COL = "Lien de la formation sur la plateforme Parcoursup"
DEFAULT_OUTFILE = "sortie_scraping.csv"
DEFAULT_DELAY = 1.0  # secondes entre requêtes

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

def detect_lang(soup):
    # langue depuis <html lang="..">
    html = soup.find("html")
    if html and html.get("lang"):
        return html.get("lang")
    # sinon via meta
    meta_lang = soup.find("meta", attrs={"http-equiv": re.compile("^content-language$", re.I)})
    if meta_lang and meta_lang.get("content"):
        return meta_lang["content"]
    return ""

def extract_pub_date(soup):
    # Essaye plusieurs schémas courants
    candidates = []
    selectors = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "article:published_time"}),
        ("meta", {"property": "og:pubdate"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publish_date"}),
        ("meta", {"itemprop": "datePublished"}),
        ("time", {"itemprop": "datePublished"}),
        ("time", {"datetime": True}),
    ]
    for tag, attrs in selectors:
        el = soup.find(tag, attrs=attrs)
        if el:
            val = el.get("content") or el.get("datetime") or el.text
            if val and len(val.strip()) >= 6:
                candidates.append(val.strip())
    return candidates[0] if candidates else ""

def extract_canonical(soup):
    link = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
    return link.get("href") if link and link.get("href") else ""

def is_allowed_by_robots(url, session, robots_cache):
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    if robots_url in robots_cache:
        rp = robots_cache[robots_url]
    else:
        rp = robotparser.RobotFileParser()
        try:
            resp = session.get(robots_url, timeout=10)
            if resp.status_code >= 400:
                # pas de robots.txt ou inaccessible => on suppose autorisé
                rp.parse("")
            else:
                rp.parse(resp.text.splitlines())
        except RequestException:
            rp.parse("")
        robots_cache[robots_url] = rp
    return rp.can_fetch(UA, url)

def normalize_url(u):
    return u.strip()

def main():
    parser = argparse.ArgumentParser(description="Scraper générique d'URLs depuis un fichier Excel vers CSV.")
    parser.add_argument("--infile", default=DEFAULT_INFILE, help="Chemin du fichier Excel en entrée")
    parser.add_argument("--sheet", default=DEFAULT_SHEET, help="Nom ou index de la feuille Excel")
    parser.add_argument("--url-col", default=DEFAULT_URL_COL, help="Nom de la colonne qui contient les URLs")
    parser.add_argument("--outfile", default=DEFAULT_OUTFILE, help="Chemin du CSV de sortie")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Délai (s) entre requêtes")
    parser.add_argument("--no-robots", action="store_true", help="Ignorer robots.txt (déconseillé)")
    parser.add_argument("--resume", action="store_true", help="Reprendre en sautant les URLs déjà présentes dans le CSV de sortie")
    args = parser.parse_args()

    in_path = Path(args.infile)
    if not in_path.exists():
        sys.exit(f"Fichier d'entrée introuvable: {in_path}")
    print(f"Lecture: {in_path} (sheet={args.sheet})")

    try:
        df = pd.read_excel(in_path, sheet_name=args.sheet)
    except Exception as e:
        sys.exit(f"Erreur de lecture Excel: {e}")

    if args.url_col not in df.columns:
        sys.exit(f"Colonne '{args.url_col}' introuvable. Colonnes disponibles: {list(df.columns)}")

    urls = (
        df[args.url_col]
        .astype(str)
        .map(normalize_url)
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    if not urls:
        sys.exit("Aucune URL détectée.")

    out_path = Path(args.outfile)
    fieldnames = [
        "source_url",
        "final_url",
        "status_code",
        "title",
        "meta_description",
        "h1",
        "lang",
        "canonical",
        "pub_date",
        "text_length",
        "error",
    ]

    processed = set()
    if args.resume and out_path.exists():
        try:
            existing = pd.read_csv(out_path)
            if "source_url" in existing.columns:
                processed = set(existing["source_url"].astype(str))
                print(f"Reprise activée: {len(processed)} URLs déjà traitées seront sautées.")
        except Exception as e:
            print(f"Impossible de lire le CSV existant pour reprise: {e}")

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8"})
    robots_cache = {}

    # Ouvre le CSV en mode ajout si reprise
    mode = "a" if args.resume and out_path.exists() else "w"
    with out_path.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()

        total = len(urls)
        for i, url in enumerate(urls, 1):
            if url in processed:
                continue

            row = {
                "source_url": url,
                "final_url": "",
                "status_code": "",
                "title": "",
                "meta_description": "",
                "h1": "",
                "lang": "",
                "canonical": "",
                "pub_date": "",
                "text_length": "",
                "error": "",
            }

            try:
                if not args.no_robots and not is_allowed_by_robots(url, session, robots_cache):
                    row["error"] = "Bloqué par robots.txt"
                    writer.writerow(row)
                    continue

                resp = session.get(url, timeout=20, allow_redirects=True)
                row["status_code"] = resp.status_code
                row["final_url"] = resp.url

                content_type = resp.headers.get("Content-Type", "")
                if resp.status_code >= 400:
                    row["error"] = f"HTTP {resp.status_code}"
                elif "text/html" not in content_type.lower():
                    row["error"] = f"Type non-HTML: {content_type}"
                else:
                    soup = BeautifulSoup(resp.text, "lxml")

                    # Titre
                    if soup.title and soup.title.string:
                        row["title"] = soup.title.string.strip()

                    # Meta description
                    md = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
                    if md and md.get("content"):
                        row["meta_description"] = md["content"].strip()

                    # H1
                    h1 = soup.find("h1")
                    if h1:
                        row["h1"] = h1.get_text(strip=True)

                    # Langue
                    row["lang"] = detect_lang(soup)

                    # Canonical
                    row["canonical"] = extract_canonical(soup)

                    # Date de publication (best-effort)
                    row["pub_date"] = extract_pub_date(soup)

                    # Longueur du texte (approx)
                    text = soup.get_text(separator=" ", strip=True)
                    row["text_length"] = len(text)

                writer.writerow(row)

            except Timeout:
                row["error"] = "Timeout"
                writer.writerow(row)
            except RequestException as e:
                row["error"] = f"Requête échouée: {type(e).__name__}"
                writer.writerow(row)
            except Exception as e:
                row["error"] = f"Erreur: {type(e).__name__}: {e}"
                writer.writerow(row)

            # Progression simple
            if i % 25 == 0 or i == total:
                print(f"[{i}/{total}] traité(s)")

            time.sleep(max(0.0, args.delay))

    print(f"Terminé. CSV écrit: {out_path.resolve()}")

if __name__ == "__main__":
    main()
