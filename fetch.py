"""
Cameron County Motivated Seller Lead Scraper
Fetches from: https://cameron.tx.publicsearch.us/
Parcel data: https://www.cameroncad.org/cad/exports/preliminary/2026/cameron-2026-GCC-preliminary-export-20260423.zip
"""

import asyncio
import json
import csv
import os
import re
import zipfile
import io
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Config ────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = 7
CLERK_BASE = "https://cameron.tx.publicsearch.us"
CAD_CSV_URL = "https://www.cameroncad.org/cad/exports/preliminary/2026/cameron-2026-GCC-preliminary-export-20260423.zip"

# Document type codes to target
TARGET_DOC_TYPES = {
    "LP":       "Lis Pendens",
    "NOFC":     "Notice of Foreclosure",
    "TAXDEED":  "Tax Deed",
    "JUD":      "Judgment",
    "CCJ":      "Certified Judgment",
    "DRJUD":    "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS":    "IRS Lien",
    "LNFED":    "Federal Lien",
    "LN":       "Lien",
    "LNMECH":   "Mechanic Lien",
    "LNHOA":    "HOA Lien",
    "MEDLN":    "Medicaid Lien",
    "PRO":      "Probate",
    "NOC":      "Notice of Commencement",
    "RELLP":    "Release Lis Pendens",
}

OUTPUT_DIRS = ["dashboard", "data"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Parcel Lookup ─────────────────────────────────────────────────────────────

def download_parcel_data():
    """Download and parse Cameron CAD simplified CSV export."""
    log.info("Downloading Cameron CAD parcel data...")
    parcel_map = {}  # owner_key -> {prop_address, prop_city, prop_state, prop_zip, mail_address, mail_city, mail_state, mail_zip}

    try:
        r = requests.get(CAD_CSV_URL, timeout=120, stream=True)
        r.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(r.content))

        # Find CSV file in zip
        csv_name = next((n for n in zf.namelist() if n.endswith('.csv') or n.endswith('.CSV')), None)
        if not csv_name:
            # Try any file
            csv_name = zf.namelist()[0] if zf.namelist() else None

        if not csv_name:
            log.warning("No CSV found in CAD zip")
            return parcel_map

        log.info(f"Parsing parcel file: {csv_name}")
        with zf.open(csv_name) as f:
            content = f.read().decode('utf-8', errors='ignore')
            reader = csv.DictReader(io.StringIO(content))

            for row in reader:
                try:
                    # Owner name - try multiple column names
                    owner = (
                        row.get('OWNER') or row.get('OWN1') or
                        row.get('OWNER_NAME') or row.get('NAME') or ''
                    ).strip().upper()

                    if not owner:
                        continue

                    # Site address
                    site_addr = (row.get('SITE_ADDR') or row.get('SITEADDR') or row.get('PROP_ADDR') or '').strip()
                    site_city = (row.get('SITE_CITY') or row.get('SITECITY') or '').strip()
                    site_state = (row.get('SITE_STATE') or row.get('STATE') or 'TX').strip()
                    site_zip  = (row.get('SITE_ZIP') or row.get('SITEZIP') or row.get('ZIP') or '').strip()

                    # Mailing address
                    mail_addr  = (row.get('ADDR_1') or row.get('MAILADR1') or row.get('MAIL_ADDR') or '').strip()
                    mail_city  = (row.get('CITY') or row.get('MAILCITY') or row.get('MAIL_CITY') or '').strip()
                    mail_state = (row.get('STATE') or row.get('MAIL_STATE') or 'TX').strip()
                    mail_zip   = (row.get('ZIP') or row.get('MAILZIP') or row.get('MAIL_ZIP') or '').strip()

                    record = {
                        'prop_address': site_addr,
                        'prop_city':    site_city,
                        'prop_state':   site_state or 'TX',
                        'prop_zip':     site_zip,
                        'mail_address': mail_addr,
                        'mail_city':    mail_city,
                        'mail_state':   mail_state or 'TX',
                        'mail_zip':     mail_zip,
                    }

                    # Index by multiple name variants
                    parts = owner.replace(',', ' ').split()
                    if len(parts) >= 2:
                        # LAST, FIRST
                        parcel_map[owner] = record
                        # FIRST LAST
                        parcel_map[f"{parts[-1]} {' '.join(parts[:-1])}"] = record
                        # LAST FIRST (no comma)
                        parcel_map[owner.replace(',', '').strip()] = record
                    else:
                        parcel_map[owner] = record

                except Exception:
                    continue

        log.info(f"Loaded {len(parcel_map)} parcel records")
    except Exception as e:
        log.error(f"Failed to download parcel data: {e}")

    return parcel_map


def lookup_parcel(owner: str, parcel_map: dict) -> dict:
    """Try multiple name variants to find parcel record."""
    if not owner:
        return {}

    owner_clean = owner.strip().upper()

    # Direct match
    if owner_clean in parcel_map:
        return parcel_map[owner_clean]

    # Try without punctuation
    owner_nopunct = re.sub(r'[^A-Z0-9 ]', ' ', owner_clean).strip()
    if owner_nopunct in parcel_map:
        return parcel_map[owner_nopunct]

    # Try partial match on last name
    last = owner_clean.split(',')[0].strip() if ',' in owner_clean else owner_clean.split()[0]
    matches = [v for k, v in parcel_map.items() if last in k]
    if matches:
        return matches[0]

    return {}


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_score(record: dict) -> tuple[int, list[str]]:
    """Compute seller motivation score 0-100 and flags list."""
    score = 30
    flags = []
    doc_type = record.get('doc_type', '')
    cat = record.get('cat', '')
    amount = record.get('amount', 0) or 0

    if isinstance(amount, str):
        amount = float(re.sub(r'[^0-9.]', '', amount) or 0)

    # Doc type flags
    if doc_type in ('LP', 'NOFC') or cat in ('LP', 'NOFC'):
        flags.append('Lis pendens')
        flags.append('Pre-foreclosure')
        score += 10

    if doc_type in ('JUD', 'CCJ', 'DRJUD') or 'JUD' in cat:
        flags.append('Judgment lien')
        score += 10

    if doc_type in ('LNCORPTX', 'LNIRS', 'LNFED', 'TAXDEED') or 'TAX' in doc_type:
        flags.append('Tax lien')
        score += 10

    if doc_type in ('LNMECH', 'LN', 'LNHOA', 'MEDLN') or doc_type == 'LN':
        flags.append('Mechanic lien' if doc_type == 'LNMECH' else 'Lien')
        score += 10

    if doc_type == 'PRO':
        flags.append('Probate / estate')
        score += 10

    # LP + FC combo bonus
    lp = any(f in flags for f in ('Lis pendens', 'Pre-foreclosure'))
    fc = 'Pre-foreclosure' in flags
    if lp and fc:
        score += 20

    # Amount bonuses
    if amount > 100000:
        score += 15
        flags.append('High debt >$100k')
    elif amount > 50000:
        score += 10
        flags.append('Debt >$50k')

    # New this week
    try:
        filed = datetime.strptime(record.get('filed', ''), '%Y-%m-%d')
        if (datetime.now() - filed).days <= 7:
            score += 5
            flags.append('New this week')
    except Exception:
        pass

    # Has address
    if record.get('prop_address'):
        score += 5
        flags.append('Has address')

    # LLC / Corp owner
    owner = record.get('owner', '')
    if any(x in owner.upper() for x in ('LLC', 'INC', 'CORP', 'LTD', 'LP ', 'L.P.')):
        flags.append('LLC / corp owner')
        score += 10

    return min(score, 100), flags


# ── Clerk Scraper ─────────────────────────────────────────────────────────────

async def scrape_clerk(lookback_days: int = 7) -> list[dict]:
    """Scrape cameron.tx.publicsearch.us for target document types."""
    records = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    date_from = start_date.strftime('%m/%d/%Y')
    date_to = end_date.strftime('%m/%d/%Y')

    log.info(f"Scraping clerk portal {date_from} → {date_to}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()

        for attempt in range(3):
            try:
                await page.goto(f"{CLERK_BASE}/search/advanced", timeout=30000)
                await page.wait_for_load_state('networkidle', timeout=15000)
                break
            except Exception as e:
                log.warning(f"Page load attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2)
        else:
            log.error("Failed to load clerk portal after 3 attempts")
            await browser.close()
            return records

        # Set date range
        try:
            await page.fill('input[placeholder*="Start"]', date_from)
            await page.fill('input[placeholder*="End"]', date_to)
        except Exception:
            pass

        # Search each doc type
        for doc_code, doc_label in TARGET_DOC_TYPES.items():
            try:
                log.info(f"Searching doc type: {doc_code} ({doc_label})")

                # Navigate to search
                await page.goto(
                    f"{CLERK_BASE}/results?_docTypes={doc_code}&department=RP"
                    f"&recordedDateRange={start_date.strftime('%Y%m%d')}%2C{end_date.strftime('%Y%m%d')}"
                    f"&searchType=quickSearch&limit=100&offset=0",
                    timeout=20000
                )
                await page.wait_for_load_state('networkidle', timeout=10000)
                await asyncio.sleep(1)

                # Parse results
                content = await page.content()
                soup = BeautifulSoup(content, 'lxml')

                # Find result cards/rows
                result_items = (
                    soup.select('.result-card') or
                    soup.select('.search-result') or
                    soup.select('tr[data-id]') or
                    soup.select('.record-row')
                )

                if not result_items:
                    # Try JSON API endpoint
                    api_url = (
                        f"{CLERK_BASE}/api/official/search?department=RP"
                        f"&docTypes={doc_code}"
                        f"&dateFrom={start_date.strftime('%Y-%m-%d')}"
                        f"&dateTo={end_date.strftime('%Y-%m-%d')}"
                        f"&limit=100&offset=0"
                    )
                    try:
                        api_resp = await page.goto(api_url, timeout=10000)
                        if api_resp:
                            body = await page.text_content('body')
                            data = json.loads(body)
                            items = data.get('records', data.get('results', data.get('data', [])))
                            for item in items:
                                rec = parse_api_record(item, doc_code, doc_label)
                                if rec:
                                    records.append(rec)
                        await page.go_back()
                    except Exception:
                        pass
                    continue

                for item in result_items:
                    try:
                        rec = parse_html_record(item, doc_code, doc_label)
                        if rec:
                            records.append(rec)
                    except Exception:
                        continue

            except Exception as e:
                log.warning(f"Error scraping {doc_code}: {e}")
                continue

        await browser.close()

    log.info(f"Scraped {len(records)} total records from clerk portal")
    return records


def parse_html_record(item, doc_code: str, doc_label: str) -> dict | None:
    """Parse an HTML result item into a record dict."""
    try:
        text = item.get_text(separator=' ', strip=True)

        # Extract doc number
        doc_num = ''
        dn_match = re.search(r'(\d{4}-\d+|\d{10,})', text)
        if dn_match:
            doc_num = dn_match.group(1)

        # Extract date
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
        filed = ''
        if date_match:
            try:
                filed = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
            except Exception:
                pass

        # Extract amount
        amt_match = re.search(r'\$[\d,]+\.?\d*', text)
        amount = 0.0
        if amt_match:
            amount = float(re.sub(r'[^0-9.]', '', amt_match.group()) or 0)

        # Extract grantor/grantee from cells or labeled fields
        grantor = ''
        grantee = ''
        cells = item.find_all(['td', 'div', 'span'])
        for i, cell in enumerate(cells):
            ct = cell.get_text(strip=True)
            if 'grantor' in cell.get('class', []) or 'grantor' in cell.get('id', '').lower():
                grantor = ct
            elif 'grantee' in cell.get('class', []) or 'grantee' in cell.get('id', '').lower():
                grantee = ct

        # Extract link
        link = item.find('a')
        clerk_url = ''
        if link and link.get('href'):
            href = link['href']
            clerk_url = href if href.startswith('http') else f"{CLERK_BASE}{href}"

        if not doc_num and not filed:
            return None

        return {
            'doc_num':   doc_num,
            'doc_type':  doc_code,
            'cat':       doc_code,
            'cat_label': doc_label,
            'filed':     filed,
            'owner':     grantor,
            'grantee':   grantee,
            'amount':    amount,
            'legal':     '',
            'clerk_url': clerk_url,
        }
    except Exception:
        return None


def parse_api_record(item: dict, doc_code: str, doc_label: str) -> dict | None:
    """Parse a JSON API record."""
    try:
        doc_num = str(item.get('docNum') or item.get('doc_num') or item.get('documentNumber') or '')
        filed_raw = item.get('recordedDate') or item.get('filed') or item.get('filedDate') or ''
        try:
            filed = datetime.strptime(filed_raw[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
        except Exception:
            filed = filed_raw[:10] if filed_raw else ''

        grantor  = item.get('grantor') or item.get('grantorName') or ''
        grantee  = item.get('grantee') or item.get('granteeName') or ''
        legal    = item.get('legalDescription') or item.get('legal') or ''
        amt_raw  = item.get('amount') or item.get('docAmount') or 0
        amount   = float(re.sub(r'[^0-9.]', '', str(amt_raw)) or 0)
        doc_id   = item.get('id') or item.get('docId') or ''
        clerk_url = f"{CLERK_BASE}/doc/{doc_id}" if doc_id else ''

        return {
            'doc_num':   doc_num,
            'doc_type':  doc_code,
            'cat':       doc_code,
            'cat_label': doc_label,
            'filed':     filed,
            'owner':     str(grantor).upper().strip(),
            'grantee':   str(grantee).upper().strip(),
            'amount':    amount,
            'legal':     legal,
            'clerk_url': clerk_url,
        }
    except Exception:
        return None


# ── Output ────────────────────────────────────────────────────────────────────

def build_output(records: list[dict], parcel_map: dict) -> dict:
    """Enrich records with parcel data, score, and build output JSON."""
    enriched = []
    for rec in records:
        try:
            parcel = lookup_parcel(rec.get('owner', ''), parcel_map)
            rec.update({
                'prop_address': parcel.get('prop_address', ''),
                'prop_city':    parcel.get('prop_city', ''),
                'prop_state':   parcel.get('prop_state', 'TX'),
                'prop_zip':     parcel.get('prop_zip', ''),
                'mail_address': parcel.get('mail_address', ''),
                'mail_city':    parcel.get('mail_city', ''),
                'mail_state':   parcel.get('mail_state', 'TX'),
                'mail_zip':     parcel.get('mail_zip', ''),
            })
            score, flags = compute_score(rec)
            rec['score'] = score
            rec['flags'] = flags
            enriched.append(rec)
        except Exception:
            continue

    enriched.sort(key=lambda r: r.get('score', 0), reverse=True)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    return {
        'fetched_at':    datetime.now().isoformat(),
        'source':        'Cameron County Clerk Portal',
        'date_range':    f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        'total':         len(enriched),
        'with_address':  sum(1 for r in enriched if r.get('prop_address')),
        'records':       enriched,
    }


def save_outputs(output: dict):
    """Save JSON to dashboard/ and data/ directories."""
    for d in OUTPUT_DIRS:
        Path(d).mkdir(parents=True, exist_ok=True)
        path = Path(d) / 'records.json'
        with open(path, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        log.info(f"Saved {output['total']} records to {path}")


def export_ghl_csv(output: dict):
    """Export GHL-compatible CSV."""
    Path('data').mkdir(parents=True, exist_ok=True)
    path = Path('data') / 'ghl_export.csv'

    fieldnames = [
        'First Name', 'Last Name', 'Mailing Address', 'Mailing City',
        'Mailing State', 'Mailing Zip', 'Property Address', 'Property City',
        'Property State', 'Property Zip', 'Lead Type', 'Document Type',
        'Date Filed', 'Document Number', 'Amount/Debt Owed', 'Seller Score',
        'Motivated Seller Flags', 'Source', 'Public Records URL',
    ]

    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in output.get('records', []):
            owner = rec.get('owner', '')
            parts = owner.replace(',', ' ').split()
            first = parts[1] if len(parts) >= 2 else ''
            last  = parts[0] if parts else owner

            writer.writerow({
                'First Name':            first,
                'Last Name':             last,
                'Mailing Address':       rec.get('mail_address', ''),
                'Mailing City':          rec.get('mail_city', ''),
                'Mailing State':         rec.get('mail_state', 'TX'),
                'Mailing Zip':           rec.get('mail_zip', ''),
                'Property Address':      rec.get('prop_address', ''),
                'Property City':         rec.get('prop_city', ''),
                'Property State':        rec.get('prop_state', 'TX'),
                'Property Zip':          rec.get('prop_zip', ''),
                'Lead Type':             rec.get('cat_label', ''),
                'Document Type':         rec.get('doc_type', ''),
                'Date Filed':            rec.get('filed', ''),
                'Document Number':       rec.get('doc_num', ''),
                'Amount/Debt Owed':      rec.get('amount', ''),
                'Seller Score':          rec.get('score', 0),
                'Motivated Seller Flags': ', '.join(rec.get('flags', [])),
                'Source':                'Cameron County Clerk',
                'Public Records URL':    rec.get('clerk_url', ''),
            })

    log.info(f"GHL CSV exported to {path}")


def push_to_apex(output: dict, apex_url: str = 'http://127.0.0.1:5001'):
    """Push high-score leads to APEX platform via API."""
    leads_pushed = 0
    for rec in output.get('records', []):
        try:
            if rec.get('score', 0) < 40:
                continue
            if not rec.get('prop_address'):
                continue

            owner = rec.get('owner', 'UNKNOWN')
            parts = owner.replace(',', ' ').split()
            if len(parts) >= 2:
                formatted_owner = f"{parts[0]}, {' '.join(parts[1:])}"
            else:
                formatted_owner = owner

            payload = {
                'owner':     formatted_owner,
                'prop_addr': f"{rec.get('prop_address', '')} {rec.get('prop_city', '')} {rec.get('prop_state', 'TX')} {rec.get('prop_zip', '')}".strip(),
                'mail_addr': f"{rec.get('mail_address', '')} {rec.get('mail_city', '')} {rec.get('mail_state', 'TX')} {rec.get('mail_zip', '')}".strip(),
                'amount':    str(rec.get('amount', '')),
                'county':    'Cameron',
                'source':    rec.get('cat_label', 'Cameron County Clerk'),
                'assessed':  '',
            }

            r = requests.post(
                f"{apex_url}/api/import_lead",
                json=payload,
                timeout=10
            )
            if r.status_code == 200:
                leads_pushed += 1
        except Exception:
            continue

    log.info(f"Pushed {leads_pushed} leads to APEX at {apex_url}")
    return leads_pushed


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=== Cameron County Motivated Seller Scraper ===")

    # 1. Download parcel data
    parcel_map = download_parcel_data()

    # 2. Scrape clerk portal
    records = await scrape_clerk(LOOKBACK_DAYS)

    # 3. Build output
    output = build_output(records, parcel_map)

    # 4. Save JSON outputs
    save_outputs(output)

    # 5. Export GHL CSV
    export_ghl_csv(output)

    # 6. Push to APEX (optional - runs locally)
    apex_url = os.environ.get('APEX_URL', 'http://127.0.0.1:5001')
    if os.environ.get('PUSH_TO_APEX', 'false').lower() == 'true':
        push_to_apex(output, apex_url)

    log.info(f"=== Done: {output['total']} records, {output['with_address']} with address ===")


if __name__ == '__main__':
    asyncio.run(main())
