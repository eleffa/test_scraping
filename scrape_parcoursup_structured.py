#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Parcoursup (fiche formation) -> CSV structuré

Entrée : un fichier Excel avec une colonne d'URLs (par défaut : "Lien de la formation sur la plateforme Parcoursup")
Sortie : un CSV avec des champs structurés (établissement, libellé, ville, frais, langues, places/voeux, liens, emails, etc.)

Stratégie :
1) Extraire g_ta_cod depuis l'URL
2) Interroger l'API OpenData MESR "fr-esr-cartographie_formations_parcoursup" (quand disponible) pour obtenir des champs fiables
3) Compléter via parsing HTML (BeautifulSoup + regex) pour les items visibles dans la page (frais, langues, places, vœux confirmés, emails…)
4) Sauvegarder un CSV (une ligne par URL). Reprise possible.

Usage minimal :
  python scrape_parcoursup_structured.py --resume

Options clés :
  --infile "liens dossier formation.xlsx"  # Excel d'entrée
  --url-col "Lien de la formation sur la plateforme Parcoursup"  # colonne des URLs
  --annee 2025  # année à cibler (opendata)
  --delay 0.7   # délai entre requêtes
"""
import sys
import csv
import re
import time
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from requests.exceptions import RequestException, Timeout
from bs4 import BeautifulSoup
import pandas as pd

DEFAULT_INFILE = "liens dossier formation.xlsx"
DEFAULT_SHEET = 0
DEFAULT_URL_COL = "Lien de la formation sur la plateforme Parcoursup"
DEFAULT_OUTFILE = "parcoursup_fiches_struct.csv"
DEFAULT_DELAY = 0.7

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept-Language": "fr,fr-FR;q=0.9,en;q=0.8"}

OD_API = "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-cartographie_formations_parcoursup/records"

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def norm_spaces(s: str) -> str:
    return " ".join(s.split())

def parse_int(s: str):
    s = s or ""
    s = s.replace("\u00A0", " ").replace(" ", "")
    return int(re.sub(r"\D+", "", s)) if re.search(r"\d", s) else None

def get_g_ta_cod(url: str):
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get("g_ta_cod") or q.get("g_ta_cod=")
        if vals:
            return str(vals[0]).strip()
    except Exception:
        pass
    return None

def opendata_fetch(g_ta_cod: str, session: requests.Session):
    params = {"limit": 1, "where": f"g_ta_cod={g_ta_cod}", "order_by": "annee DESC"}
    try:
        r = session.get(OD_API, params=params, timeout=20)
        if r.status_code == 200:
            out = r.json()
            recs = out.get("results") or []
            if recs:
                return recs[0]
    except Exception:
        pass
    return {}

def extract_text_after_heading(soup: BeautifulSoup, heading_regex: str, max_chars=400):
    pat = re.compile(heading_regex, re.I)
    tags = soup.find_all(lambda t: t.name in ["h1","h2","h3","h4","h5","h6","strong"] and t.get_text(strip=True) and pat.search(t.get_text(" ", strip=True)))
    if not tags:
        return ""
    t = tags[0]
    texts = []
    for el in t.find_all_next():
        if el == t:
            continue
        if el.name in ["h1","h2","h3","h4","h5","h6","strong"]:
            break
        if el.name in ["p","li","div","span","dd"]:
            txt = norm_spaces(el.get_text(" ", strip=True))
            if txt:
                texts.append(txt)
        if sum(len(x) for x in texts) > max_chars:
            break
    return norm_spaces(" ".join(texts))

def collect_section_text(soup, heading_patterns, max_chars=4000):
    """
    Trouve un bloc de contenu qui suit un titre correspondant à l'un des patterns.
    Concatène textes des p/li/div/td jusqu'au prochain titre.
    """
    pats = [re.compile(p, re.I) for p in heading_patterns]
    def is_heading(t):
        return t.name in ["h1","h2","h3","h4","h5","h6","strong"]
    # Chercher le premier titre qui matche
    for tag in soup.find_all(lambda t: is_heading(t) and t.get_text(strip=True)):
        txt = tag.get_text(" ", strip=True)
        if any(p.search(txt) for p in pats):
            # collecter le contenu suivant
            chunks = []
            for el in tag.find_all_next():
                if el == tag: 
                    continue
                if is_heading(el):
                    break
                if el.name in ["p","li","div","span","dd","td","th"]:
                    ttxt = el.get_text(" ", strip=True)
                    if ttxt:
                        chunks.append(ttxt)
                if sum(len(x) for x in chunks) > max_chars:
                    break
            return norm_spaces(" ".join(chunks))
    return ""

def parse_html_fields(html: str):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    # Frais de scolarité
    frais_annee = ""
    frais_boursiers = ""
    bloc_frais = extract_text_after_heading(soup, r"Frais\s+de\s+scolarité")
    if not bloc_frais:
        bloc_frais = "\n".join([l for l in text.splitlines() if "Frais de scolarité" in l or "Par année" in l])
    if bloc_frais:
        m = re.search(r"Par année\s*:?[\s\-]*([0-9\.\s\u00A0€euros]+)", bloc_frais, flags=re.I)
        if m:
            frais_annee = norm_spaces(m.group(1).replace("euros", "€"))
        m2 = re.search(r"boursier[s]?\s*[:\-]?\s*(.+?)(?:$|\.)", bloc_frais, flags=re.I)
        if m2:
            frais_boursiers = norm_spaces(m2.group(1))

    # Langues et options
    lv1 = lv2 = niveau_fr = ""
    bloc_langues = extract_text_after_heading(soup, r"Langues?\s+et\s+options")
    if bloc_langues:
        m = re.search(r"Langue vivante 1\s*:\s*(.+?)(?:\s{2,}|$)", bloc_langues, flags=re.I)
        if m: lv1 = m.group(1).strip(" .;")
        m = re.search(r"Langue vivante 2\s*:\s*(.+?)(?:\s{2,}|$)", bloc_langues, flags=re.I)
        if m: lv2 = m.group(1).strip(" .;")
        m = re.search(r"Niveau de français requis.*?:\s*([A-C][12])", bloc_langues, flags=re.I)
        if m: niveau_fr = m.group(1)

    # Places et vœux confirmés
    places = None
    voeux_confirmes = None
    top_block = text
    m_places = re.search(r"(\d[\d\s\u00A0]*)\s+places?\s+en\s+\d{4}", top_block, flags=re.I)
    if m_places:
        places = parse_int(m_places.group(1))
    m_voeux = re.search(r"(\d[\d\s\u00A0]*)\s+v[œo]ux\s+confirm[ée]s?\s+en\s+\d{4}", top_block, flags=re.I)
    if m_voeux:
        voeux_confirmes = parse_int(m_voeux.group(1))

    # Chiffres globaux
    candidats_postules = propositions = integ = None
    m = re.search(r"(\d[\d\s\u00A0]*)\s+candidats?\s+ont\s+postulé", text, flags=re.I)
    if m: candidats_postules = parse_int(m.group(1))
    m = re.search(r"(\d[\d\s\u00A0]*)\s+candidats?\s+ont\s+pu\s+recevoir\s+une\s+proposition", text, flags=re.I)
    if m: propositions = parse_int(m.group(1))
    m = re.search(r"(\d[\d\s\u00A0]*)\s+candidats?\s+ont\s+choisi\s+d'intégrer", text, flags=re.I)
    if m: integ = parse_int(m.group(1))

    # Liens
    onisep_url = ""
    cat_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = a.get_text(" ", strip=True).lower()
        if "onisep" in href or "onisep" in label:
            onisep_url = href
        if "catalogue" in label or "catalogue" in href or "formations.u-" in href:
            cat_url = href

    # Emails
    emails = sorted(set(re.findall(EMAIL_RE, text)))

    # Titre
    titre_bloc = ""
    if soup.title and soup.title.string:
        titre_bloc = norm_spaces(soup.title.string)

    # ---- Sections demandées ----
    criteres_analyse = collect_section_text(
        soup,
        [
            r"Comprendre\s+les\s+crit[eè]res\s+d'?analyse\s+des\s+candidatures",
            r"Crit[eè]res\s+d'?analyse\s+des\s+candidatures",
        ]
    )
    chiffres_acces = collect_section_text(
        soup,
        [
            r"Consulter\s+les\s+chiffres\s+d[’']acc[eè]s?\s+\w*\s+la\s+formation",
            r"Chiffres\s+d[’']acc[eè]s?\s+\w*\s+la\s+formation",
            r"Les\s+chiffres\s+globaux\s+d[’']acc[eè]s",
        ]
    )
    poursuites_etudes = collect_section_text(
        soup,
        [
            r"Poursuivre\s+ses\s+[eé]tudes",
            r"Poursuites?\s+d[’']?[eé]tudes",
        ]
    )
    debouches = collect_section_text(
        soup,
        [
            r"conn[aîi]tre\s+les\s+d[eé]bouch[ée]s",
            r"D[eé]bouch[ée]s",
        ]
    )
    contacter = collect_section_text(
        soup,
        [
            r"Contacter\s+et\s+[eé]changer\s+avec\s+l[’']?[eé]tablissement",
            r"Contacts?\s+et\s+[eé]changes?",
            r"Contact",
        ]
    )

    return {
        "frais_annee": frais_annee,
        "frais_boursiers": frais_boursiers,
        "lv1": lv1,
        "lv2": lv2,
        "niveau_francais": niveau_fr,
        "places": places,
        "voeux_confirmes": voeux_confirmes,
        "candidats_postules": candidats_postules,
        "propositions": propositions,
        "integres": integ,
        "onisep_url": onisep_url,
        "catalogue_url": cat_url,
        "emails_contact": ";".join(emails) if emails else "",
        "criteres_analyse": criteres_analyse,
        "chiffres_acces": chiffres_acces,
        "poursuites_etudes": poursuites_etudes,
        "debouches": debouches,
        "contacter_etablissement": contacter,
        "titre_bloc": titre_bloc,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", default=DEFAULT_INFILE)
    ap.add_argument("--sheet", default=DEFAULT_SHEET)
    ap.add_argument("--url-col", default=DEFAULT_URL_COL)
    ap.add_argument("--outfile", default=DEFAULT_OUTFILE)
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    in_path = Path(args.infile)
    if not in_path.exists():
        sys.exit(f"Fichier d'entrée introuvable: {in_path}")

    try:
        df = pd.read_excel(in_path, sheet_name=args.sheet)
    except Exception as e:
        sys.exit(f"Erreur de lecture Excel: {e}")

    if args.url_col not in df.columns:
        sys.exit(f"Colonne '{args.url_col}' introuvable. Colonnes dispo: {list(df.columns)}")

    urls = (
        df[args.url_col]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    if not urls:
        sys.exit("Aucune URL à traiter.")

    out_path = Path(args.outfile)
    fieldnames = [
        "source_url",
        "g_ta_cod",
        # OpenData
        "od_libelle_formation",
        "od_libelle_etablissement",
        "od_diplome",
        "od_secteur",
        "od_academie",
        "od_departement",
        "od_commune",
        "od_code_postal",
        "od_uai",
        # HTML/Fallback
        "titre_bloc",
        "places",
        "voeux_confirmes",
        "candidats_postules",
        "propositions",
        "integres",
        "frais_annee",
        "frais_boursiers",
        "lv1",
        "lv2",
        "niveau_francais",
        "onisep_url",
        "catalogue_url",
        "emails_contact",
        "criteres_analyse",
        "chiffres_acces",
        "poursuites_etudes",
        "debouches",
        "contacter_etablissement",
        # Statut requête
        "http_status",
        "error",
    ]

    mode = "a" if args.resume and out_path.exists() else "w"
    done = set()
    if mode == "a":
        try:
            ex = pd.read_csv(out_path, usecols=["source_url"])
            done = set(ex["source_url"].astype(str))
        except Exception:
            pass

    session = requests.Session()
    session.headers.update(HEADERS)

    with out_path.open(mode, newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            wr.writeheader()

        for i, url in enumerate(urls, 1):
            if url in done:
                continue

            row = dict.fromkeys(fieldnames, "")
            row["source_url"] = url

            try:
                gid = get_g_ta_cod(url)
                row["g_ta_cod"] = gid or ""

                # OpenData
                od = opendata_fetch(gid, session) if gid else {}
                if od:
                    row["od_libelle_formation"] = od.get("libelle_formation") or od.get("libelle_long") or ""
                    row["od_libelle_etablissement"] = od.get("libelle_etablissement") or od.get("etablissement") or ""
                    row["od_diplome"] = od.get("type_de_formation") or od.get("diplome") or ""
                    row["od_secteur"] = od.get("secteur") or ""
                    row["od_academie"] = od.get("academie") or od.get("nom_academie") or ""
                    row["od_departement"] = od.get("departement") or od.get("nom_departement") or ""
                    row["od_commune"] = od.get("commune") or ""
                    row["od_code_postal"] = od.get("code_postal") or ""
                    row["od_uai"] = od.get("uai") or ""

                # HTML
                try:
                    r = session.get(url, timeout=25)
                    row["http_status"] = r.status_code
                    if r.status_code == 200 and "text/html" in (r.headers.get("Content-Type","").lower()):
                        try:
                            parsed = parse_html_fields(r.text)
                            for k, v in parsed.items():
                                row[k] = v
                        except Exception as e:
                            row["error"] = f"Parse error: {type(e).__name__}: {e}"
                except Timeout:
                    row["error"] = "Timeout"
                except RequestException as e:
                    row["error"] = f"HTTP error: {type(e).__name__}"

            except Exception as e:
                row["error"] = f"Erreur: {type(e).__name__}: {e}"

            wr.writerow(row)

            if i % 25 == 0:
                print(f"[{i}/{len(urls)}] traités")
            time.sleep(max(0.0, args.delay))

    print(f"Terminé. CSV écrit: {out_path.resolve()}")

if __name__ == "__main__":
    main()
