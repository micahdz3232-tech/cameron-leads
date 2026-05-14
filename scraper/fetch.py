import asyncio, json, csv, os, re, io, logging, zipfile
from datetime import datetime, timedelta
from pathlib import Path
import requests

LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '30'))
CLERK_BASE = "https://cameron.tx.publicsearch.us"
GDRIVE_ID = "1jG1jwlCyX6bAAwWU4TuAD-k0dykWiOk3"

TARGET_DOC_TYPES = {
    "LP":"Lis Pendens","NOFC":"Notice of Foreclosure","TAXDEED":"Tax Deed",
    "JUD":"Judgment","CCJ":"Certified Judgment","DRJUD":"Domestic Judgment",
    "LNCORPTX":"Corp Tax Lien","LNIRS":"IRS Lien","LNFED":"Federal Lien",
    "LN":"Lien","LNMECH":"Mechanic Lien","LNHOA":"HOA Lien",
    "MEDLN":"Medicaid Lien","PRO":"Probate","NOC":"Notice of Commencement","RELLP":"Release Lis Pendens",
}
OUTPUT_DIRS = ["docs", "data"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def download_parcel_data():
    log.info("Downloading parcel data from Google Drive...")
    parcel_map = {}
    try:
        session = requests.Session()
        # Step 1: get confirm token for large file
        url = f"https://drive.google.com/uc?export=download&id={GDRIVE_ID}"
        r = session.get(url, timeout=30)
        # Find confirm token
        confirm = re.search(r'confirm=([^&"]+)', r.text)
        if confirm:
            token = confirm.group(1)
            url2 = f"https://drive.google.com/uc?export=download&id={GDRIVE_ID}&confirm={token}"
            log.info(f"Using confirm token: {token[:10]}...")
            r = session.get(url2, timeout=180, stream=True)
        else:
            # Try direct download
            r = session.get(f"https://drive.usercontent.google.com/download?id={GDRIVE_ID}&export=download&confirm=t", timeout=180, stream=True)

        r.raise_for_status()
        content = r.content
        log.info(f"Downloaded {len(content)} bytes")

        # Check if it's actually a zip
        if content[:2] != b'PK':
            log.warning(f"Not a zip file, first bytes: {content[:20]}")
            # Try usercontent URL
            r2 = session.get(f"https://drive.usercontent.google.com/download?id={GDRIVE_ID}&export=download&confirm=t&authuser=0", timeout=180, stream=True)
            content = r2.content
            log.info(f"Retry downloaded {len(content)} bytes, starts with: {content[:4]}")

        if content[:2] == b'PK':
            # Try as zip first
            try:
                zf = zipfile.ZipFile(io.BytesIO(content))
                csv_name = next((n for n in zf.namelist() if n.lower().endswith('.csv')), None)
                if csv_name:
                    with zf.open(csv_name) as f:
                        text = f.read().decode('utf-8', errors='ignore')
                else:
                    # Try xlsx inside zip
                    xlsx_name = next((n for n in zf.namelist() if n.lower().endswith('.xlsx')), None)
                    if xlsx_name:
                        import openpyxl
                        wb = openpyxl.load_workbook(io.BytesIO(zf.read(xlsx_name)))
                        ws = wb.active
                        rows_data = list(ws.rows)
                        headers = [str(c.value or '').strip() for c in rows_data[0]]
                        text_rows = []
                        text_rows.append(','.join(headers))
                        for row in rows_data[1:]:
                            text_rows.append(','.join([str(c.value or '').replace(',','') for c in row]))
                        text = '
'.join(text_rows)
                    else:
                        log.warning(f'No CSV or XLSX in zip: {zf.namelist()[:5]}')
                        return parcel_map
            except Exception as ze:
                log.warning(f'Not a zip: {ze} - trying as xlsx directly')
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(io.BytesIO(content))
                    ws = wb.active
                    rows_data = list(ws.rows)
                    headers = [str(c.value or '').strip() for c in rows_data[0]]
                    log.info(f'Excel columns: {headers[:10]}')
                    text_rows = [','.join(headers)]
                    for row in rows_data[1:]:
                        text_rows.append(','.join([str(c.value or '').replace(',','') for c in row]))
                    text = '
'.join(text_rows)
                except Exception as xe:
                    log.error(f'Excel parse failed: {xe}')
                    return parcel_map
            reader = csv.DictReader(io.StringIO(text))
                cols = reader.fieldnames or []
                log.info(f"CSV columns: {cols[:10]}")
                for row in reader:
                    try:
                        owner = (row.get('OWNER') or row.get('OWN1') or row.get('OWNER_NAME') or '').strip().upper()
                        if not owner: continue
                        record = {
                            'prop_address': (row.get('SITE_ADDR') or row.get('SITEADDR') or row.get('PROP_ADDR') or '').strip(),
                            'prop_city':    (row.get('SITE_CITY') or row.get('SITECITY') or 'BROWNSVILLE').strip(),
                            'prop_state':   'TX',
                            'prop_zip':     (row.get('SITE_ZIP') or row.get('SITEZIP') or '').strip(),
                            'mail_address': (row.get('ADDR_1') or row.get('MAILADR1') or row.get('MAIL_ADDR') or '').strip(),
                            'mail_city':    (row.get('CITY') or row.get('MAILCITY') or '').strip(),
                            'mail_state':   (row.get('STATE') or 'TX').strip(),
                            'mail_zip':     (row.get('ZIP') or row.get('MAILZIP') or '').strip(),
                        }
                        parcel_map[owner] = record
                        last = owner.split(',')[0].strip()
                        if last and last not in parcel_map:
                            parcel_map[last] = record
                    except: continue
            log.info(f"Loaded {len(parcel_map)} parcel records")
        else:
            log.error("Could not download valid zip file from Google Drive")
    except Exception as e:
        log.warning(f"Parcel download failed: {e}")
    return parcel_map

def lookup_parcel(owner, parcel_map):
    if not owner or not parcel_map: return {}
    ou = owner.strip().upper()
    if ou in parcel_map: return parcel_map[ou]
    return parcel_map.get(ou.split(',')[0].strip(), {})

def compute_score(record):
    score, flags = 30, []
    dt = record.get('doc_type', '')
    amount = record.get('amount', 0) or 0
    if isinstance(amount, str):
        try: amount = float(re.sub(r'[^0-9.]', '', amount) or 0)
        except: amount = 0
    if dt in ('LP','RELLP'): flags.append('Lis pendens'); score += 10
    if dt == 'NOFC': flags.append('Pre-foreclosure'); score += 10
    if dt in ('JUD','CCJ','DRJUD'): flags.append('Judgment lien'); score += 10
    if dt in ('LNCORPTX','LNIRS','LNFED','TAXDEED'): flags.append('Tax lien'); score += 10
    if dt in ('LN','LNMECH','LNHOA','MEDLN'): flags.append('Lien'); score += 10
    if dt == 'PRO': flags.append('Probate / estate'); score += 10
    if 'Lis pendens' in flags and 'Pre-foreclosure' in flags: score += 20
    if amount > 100000: score += 15; flags.append('High debt >$100k')
    elif amount > 50000: score += 10; flags.append('Debt >$50k')
    try:
        filed = datetime.strptime(record.get('filed',''), '%Y-%m-%d')
        if (datetime.now()-filed).days <= 7: score += 5; flags.append('New this week')
    except: pass
    if record.get('prop_address'): score += 5; flags.append('Has address')
    if any(x in record.get('owner','').upper() for x in ('LLC','INC','CORP','LTD','L.P.')): flags.append('LLC / corp owner'); score += 10
    return min(score, 100), flags

async def scrape_with_playwright(lookback_days=30):
    from playwright.async_api import async_playwright
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    date_from = start_date.strftime('%Y%m%d')
    date_to = end_date.strftime('%Y%m%d')
    log.info(f"Scraping {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    api_records = []
    html_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox','--disable-dev-shm-usage','--disable-gpu']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width':1920,'height':1080}
        )
        page = await context.new_page()

        # Intercept XHR responses
        async def handle_response(response):
            try:
                url = response.url
                if response.status != 200: return
                ct = response.headers.get('content-type','')
                if 'json' not in ct: return
                data = await response.json()
                items = []
                if isinstance(data, list): items = data
                elif isinstance(data, dict):
                    for key in ['records','results','data','items','searchResults','documents','hits']:
                        if data.get(key) and isinstance(data[key], list):
                            items = data[key]
                            break
                if not items: return
                log.info(f"XHR intercepted: {url[:70]} -> {len(items)} items")
                for item in items:
                    if not isinstance(item, dict): continue
                    dt = (item.get('docType') or item.get('doc_type') or
                          item.get('documentType') or item.get('type') or '')
                    dtl = TARGET_DOC_TYPES.get(dt, dt or 'Record')
                    doc_num = str(item.get('docNum') or item.get('instrumentNumber') or
                                 item.get('id') or item.get('docId') or '')
                    filed_raw = (item.get('recordedDate') or item.get('filed') or
                                item.get('instrumentDate') or item.get('date') or '')
                    try: filed = datetime.strptime(str(filed_raw)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
                    except:
                        try: filed = datetime.strptime(str(filed_raw)[:10], '%m/%d/%Y').strftime('%Y-%m-%d')
                        except: filed = str(filed_raw)[:10] if filed_raw else ''
                    grantor = str(item.get('grantor') or item.get('grantorName') or
                                 item.get('name') or item.get('owner') or '').upper().strip()
                    grantee = str(item.get('grantee') or item.get('granteeName') or '').upper().strip()
                    try: amount = float(re.sub(r'[^0-9.]','',str(item.get('amount') or item.get('docAmount') or 0)) or 0)
                    except: amount = 0.0
                    doc_id = item.get('id') or item.get('docId') or doc_num
                    clerk_url = (item.get('url') or item.get('link') or
                                f"{CLERK_BASE}/doc/{doc_id}" if doc_id else '')
                    api_records.append({
                        'doc_num': doc_num, 'doc_type': dt, 'cat': dt, 'cat_label': dtl,
                        'filed': filed, 'owner': grantor, 'grantee': grantee,
                        'amount': amount, 'legal': str(item.get('legalDescription',''))[:200],
                        'clerk_url': clerk_url
                    })
            except Exception as e:
                pass

        page.on('response', handle_response)
        doc_types = ','.join(TARGET_DOC_TYPES.keys())

        # Main bulk search
        main_url = f"{CLERK_BASE}/results?department=RP&_docTypes={doc_types}&recordedDateRange={date_from},{date_to}&searchType=quickSearch&limit=500&offset=0&viewType=list"
        log.info("Loading main search page...")
        try:
            await page.goto(main_url, timeout=60000, wait_until='networkidle')
            # Wait for loading spinner to disappear
            try:
                await page.wait_for_selector('.loading, [class*="loading"], [class*="spinner"]', state='hidden', timeout=15000)
            except: pass
            await asyncio.sleep(8)
            log.info(f"After main load: {len(api_records)} API records")
        except Exception as e:
            log.warning(f"Main load: {e}")

        # Individual doc type searches
        log.info("Individual doc type searches...")
        for doc_code, doc_label in TARGET_DOC_TYPES.items():
            before = len(api_records)
            try:
                iurl = f"{CLERK_BASE}/results?department=RP&_docTypes={doc_code}&recordedDateRange={date_from},{date_to}&searchType=quickSearch&limit=200&offset=0&viewType=list"
                await page.goto(iurl, timeout=30000, wait_until='domcontentloaded')
                try:
                    await page.wait_for_selector('.loading, [class*="loading"]', state='hidden', timeout=10000)
                except: pass
                await asyncio.sleep(6)

                # Parse HTML table
                rows = await page.query_selector_all('tbody tr')
                if not rows:
                    rows = await page.query_selector_all('[class*="result-row"], [class*="record-row"]')

                for row in rows:
                    try:
                        cells = []
                        tds = await row.query_selector_all('td')
                        for td in tds:
                            t = await td.inner_text()
                            cells.append(t.strip())
                        if len(cells) < 2: continue

                        # Try to find date
                        all_text = ' '.join(cells)
                        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', all_text)
                        filed = ''
                        if date_match:
                            try: filed = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                            except: pass

                        # Find link
                        link = await row.query_selector('a')
                        clerk_url = ''
                        if link:
                            href = await link.get_attribute('href') or ''
                            clerk_url = href if href.startswith('http') else f"{CLERK_BASE}{href}" if href else ''

                        # Find amount
                        amt_match = re.search(r'\$[\d,]+\.?\d*', all_text)
                        amount = 0.0
                        if amt_match:
                            try: amount = float(re.sub(r'[^0-9.]', '', amt_match.group()) or 0)
                            except: pass

                        # Best guess at owner from cells
                        owner = ''
                        for cell in cells:
                            if len(cell) > 5 and not re.match(r'^\d', cell) and '$' not in cell and '/' not in cell:
                                owner = cell.upper()
                                break

                        doc_num = cells[0] if cells else ''

                        html_records.append({
                            'doc_num': doc_num, 'doc_type': doc_code, 'cat': doc_code,
                            'cat_label': doc_label, 'filed': filed, 'owner': owner,
                            'grantee': cells[-1].upper() if cells else '', 'amount': amount,
                            'legal': '', 'clerk_url': clerk_url
                        })
                    except: continue

                after = len(api_records)
                html_count = len(html_records)
                if after > before or html_count > 0:
                    log.info(f"{doc_code}: +{after-before} API, {html_count} HTML total")
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning(f"{doc_code}: {e}")
                continue

        await browser.close()

    # Combine and deduplicate
    all_records = api_records + html_records
    log.info(f"Raw: {len(api_records)} API + {len(html_records)} HTML = {len(all_records)} total")

    seen = set()
    unique = []
    for r in all_records:
        # Use doc_num + date as key, fallback to index
        key = f"{r.get('doc_num','')}-{r.get('filed','')}-{r.get('doc_type','')}"
        if not key.strip('-') or key in seen:
            if key in seen: continue
        seen.add(key)
        unique.append(r)

    log.info(f"Unique records: {len(unique)}")
    return unique

def build_output(records, parcel_map=None):
    if parcel_map is None: parcel_map = {}
    enriched = []
    for rec in records:
        try:
            parcel = lookup_parcel(rec.get('owner',''), parcel_map)
            if parcel:
                rec.update({
                    'prop_address': parcel.get('prop_address',''),
                    'prop_city':    parcel.get('prop_city',''),
                    'prop_state':   parcel.get('prop_state','TX'),
                    'prop_zip':     parcel.get('prop_zip',''),
                    'mail_address': parcel.get('mail_address',''),
                    'mail_city':    parcel.get('mail_city',''),
                    'mail_state':   parcel.get('mail_state','TX'),
                    'mail_zip':     parcel.get('mail_zip',''),
                })
            for k,v in [('prop_address',''),('prop_city',''),('prop_state','TX'),('prop_zip',''),('mail_address',''),('mail_city',''),('mail_state','TX'),('mail_zip','')]:
                rec.setdefault(k, v)
            score, flags = compute_score(rec)
            rec['score'] = score
            rec['flags'] = flags
            enriched.append(rec)
        except: continue
    enriched.sort(key=lambda r: r.get('score',0), reverse=True)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    return {
        'fetched_at': datetime.now().isoformat(),
        'source': 'Cameron County Clerk Portal',
        'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
        'total': len(enriched),
        'with_address': sum(1 for r in enriched if r.get('prop_address')),
        'records': enriched,
    }

def save_outputs(output):
    for d in OUTPUT_DIRS:
        Path(d).mkdir(parents=True, exist_ok=True)
        path = Path(d)/'records.json'
        with open(path,'w') as f: json.dump(output, f, indent=2, default=str)
        log.info(f"Saved {output['total']} records to {path}")

def export_ghl_csv(output):
    Path('data').mkdir(parents=True, exist_ok=True)
    fieldnames = ['First Name','Last Name','Mailing Address','Mailing City','Mailing State','Mailing Zip','Property Address','Property City','Property State','Property Zip','Lead Type','Document Type','Date Filed','Document Number','Amount/Debt Owed','Seller Score','Motivated Seller Flags','Source','Public Records URL']
    with open('data/ghl_export.csv','w',newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in output.get('records',[]):
            owner = rec.get('owner','')
            parts = owner.replace(',',' ').split()
            writer.writerow({
                'First Name': parts[1] if len(parts)>=2 else '',
                'Last Name': parts[0] if parts else owner,
                'Mailing Address': rec.get('mail_address',''),
                'Mailing City': rec.get('mail_city',''),
                'Mailing State': rec.get('mail_state','TX'),
                'Mailing Zip': rec.get('mail_zip',''),
                'Property Address': rec.get('prop_address',''),
                'Property City': rec.get('prop_city',''),
                'Property State': rec.get('prop_state','TX'),
                'Property Zip': rec.get('prop_zip',''),
                'Lead Type': rec.get('cat_label',''),
                'Document Type': rec.get('doc_type',''),
                'Date Filed': rec.get('filed',''),
                'Document Number': rec.get('doc_num',''),
                'Amount/Debt Owed': rec.get('amount',''),
                'Seller Score': rec.get('score',0),
                'Motivated Seller Flags': ', '.join(rec.get('flags',[])),
                'Source': 'Cameron County Clerk',
                'Public Records URL': rec.get('clerk_url',''),
            })
    log.info("GHL CSV exported")

def main():
    log.info("=== Cameron County Motivated Seller Scraper ===")
    parcel_map = download_parcel_data()
    records = asyncio.run(scrape_with_playwright(LOOKBACK_DAYS))
    output = build_output(records, parcel_map)
    save_outputs(output)
    export_ghl_csv(output)
    log.info(f"=== Done: {output['total']} records, {output['with_address']} with address ===")

if __name__ == '__main__':
    main()
