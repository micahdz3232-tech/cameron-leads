import asyncio, json, csv, os, re, zipfile, io, time, logging
from datetime import datetime, timedelta
from pathlib import Path
import requests

LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '30'))
CLERK_BASE = "https://cameron.tx.publicsearch.us"

TARGET_DOC_TYPES = {"LP":"Lis Pendens","NOFC":"Notice of Foreclosure","TAXDEED":"Tax Deed","JUD":"Judgment","CCJ":"Certified Judgment","DRJUD":"Domestic Judgment","LNCORPTX":"Corp Tax Lien","LNIRS":"IRS Lien","LNFED":"Federal Lien","LN":"Lien","LNMECH":"Mechanic Lien","LNHOA":"HOA Lien","MEDLN":"Medicaid Lien","PRO":"Probate","NOC":"Notice of Commencement","RELLP":"Release Lis Pendens"}
OUTPUT_DIRS = ["docs", "data"]
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def compute_score(record):
    score, flags = 30, []
    dt = record.get("doc_type", "")
    amount = record.get("amount", 0) or 0
    if isinstance(amount, str):
        try: amount = float(re.sub(r"[^0-9.]", "", amount) or 0)
        except: amount = 0
    if dt in ("LP","RELLP"): flags.append("Lis pendens"); score += 10
    if dt == "NOFC": flags.append("Pre-foreclosure"); score += 10
    if dt in ("JUD","CCJ","DRJUD"): flags.append("Judgment lien"); score += 10
    if dt in ("LNCORPTX","LNIRS","LNFED","TAXDEED"): flags.append("Tax lien"); score += 10
    if dt in ("LN","LNMECH","LNHOA","MEDLN"): flags.append("Mechanic lien" if dt=="LNMECH" else "Lien"); score += 10
    if dt == "PRO": flags.append("Probate / estate"); score += 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score += 20
    if amount > 100000: score += 15; flags.append("High debt >$100k")
    elif amount > 50000: score += 10; flags.append("Debt >$50k")
    try:
        filed = datetime.strptime(record.get("filed",""), "%Y-%m-%d")
        if (datetime.now()-filed).days <= 7: score += 5; flags.append("New this week")
    except: pass
    if record.get("prop_address"): score += 5; flags.append("Has address")
    if any(x in record.get("owner","").upper() for x in ("LLC","INC","CORP","LTD","L.P.")): flags.append("LLC / corp owner"); score += 10
    return min(score, 100), flags

async def scrape_with_playwright(lookback_days=30):
    from playwright.async_api import async_playwright
    records = []
    api_records = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    date_from = start_date.strftime("%Y%m%d")
    date_to = end_date.strftime("%Y%m%d")
    log.info(f"Scraping {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":800}
        )
        page = await context.new_page()

        async def handle_response(response):
            try:
                if response.status == 200 and "json" in response.headers.get("content-type",""):
                    data = await response.json()
                    items = data.get("records") or data.get("results") or data.get("data") or []
                    if isinstance(data, list): items = data
                    for item in items:
                        if isinstance(item, dict):
                            dt = item.get("docType") or item.get("doc_type") or item.get("documentType") or ""
                            dtl = TARGET_DOC_TYPES.get(dt, dt)
                            doc_num = str(item.get("docNum") or item.get("instrumentNumber") or item.get("id") or "")
                            filed_raw = item.get("recordedDate") or item.get("filed") or item.get("instrumentDate") or ""
                            try: filed = datetime.strptime(str(filed_raw)[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                            except:
                                try: filed = datetime.strptime(str(filed_raw)[:10], "%m/%d/%Y").strftime("%Y-%m-%d")
                                except: filed = str(filed_raw)[:10] if filed_raw else ""
                            grantor = str(item.get("grantor") or item.get("grantorName") or item.get("name") or "").upper().strip()
                            grantee = str(item.get("grantee") or item.get("granteeName") or "").upper().strip()
                            try: amount = float(re.sub(r"[^0-9.]", "", str(item.get("amount") or item.get("docAmount") or 0)) or 0)
                            except: amount = 0.0
                            doc_id = item.get("id") or item.get("docId") or doc_num
                            clerk_url = item.get("url") or item.get("link") or (f"{CLERK_BASE}/doc/{doc_id}" if doc_id else "")
                            if doc_num or filed or grantor:
                                api_records.append({"doc_num":doc_num,"doc_type":dt,"cat":dt,"cat_label":dtl,"filed":filed,"owner":grantor,"grantee":grantee,"amount":amount,"legal":str(item.get("legalDescription") or "")[:200],"clerk_url":clerk_url})
                    if api_records: log.info(f"API intercepted: {len(api_records)} records so far")
            except: pass

        page.on("response", handle_response)
        doc_types = ",".join(TARGET_DOC_TYPES.keys())

        for attempt in range(3):
            try:
                url = f"{CLERK_BASE}/results?department=RP&_docTypes={doc_types}&recordedDateRange={date_from},{date_to}&searchType=quickSearch&limit=500&offset=0&viewType=list"
                log.info(f"Loading page (attempt {attempt+1})...")
                await page.goto(url, timeout=60000, wait_until="networkidle")
                await asyncio.sleep(8)

                # Scroll to load all results
                for _ in range(5):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)

                rows = await page.query_selector_all("tbody tr")
                log.info(f"Found {len(rows)} HTML rows after scroll")

                for row in rows:
                    try:
                        text = await row.inner_text()
                        if len(text.strip()) < 3: continue
                        cells = [c.strip() for c in text.split("\t") if c.strip()]
                        if len(cells) < 2: cells = [c.strip() for c in text.split("\n") if c.strip()]
                        date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                        filed = ""
                        if date_match:
                            try: filed = datetime.strptime(date_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                            except: pass
                        amt_match = re.search(r"\$[\d,]+", text)
                        amount = 0.0
                        if amt_match:
                            try: amount = float(re.sub(r"[^0-9.]", "", amt_match.group()) or 0)
                            except: pass
                        link = await row.query_selector("a")
                        clerk_url = ""
                        if link:
                            href = await link.get_attribute("href") or ""
                            clerk_url = href if href.startswith("http") else f"{CLERK_BASE}{href}" if href else ""
                        if filed or (cells and len(cells[0]) > 3):
                            records.append({"doc_num":cells[0] if cells else "","doc_type":"","cat":"","cat_label":"Court Record","filed":filed,"owner":cells[2] if len(cells)>2 else "","grantee":cells[3] if len(cells)>3 else "","amount":amount,"legal":"","clerk_url":clerk_url})
                    except: continue

                if records or api_records:
                    log.info(f"Got {len(records)} HTML + {len(api_records)} API records")
                    break
            except Exception as e:
                log.warning(f"Attempt {attempt+1}: {e}")
                await asyncio.sleep(3)

        # Individual doc type searches for more coverage
        log.info("Running individual doc type searches for more coverage...")
        for doc_code, doc_label in TARGET_DOC_TYPES.items():
            try:
                url = f"{CLERK_BASE}/results?department=RP&_docTypes={doc_code}&recordedDateRange={date_from},{date_to}&searchType=quickSearch&limit=200&offset=0&viewType=list"
                await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                await asyncio.sleep(4)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                rows = await page.query_selector_all("tbody tr")
                count = 0
                for row in rows:
                    try:
                        text = await row.inner_text()
                        if len(text.strip()) < 3: continue
                        cells = [c.strip() for c in text.split("\t") if c.strip()]
                        if len(cells) < 2: cells = [c.strip() for c in text.split("\n") if c.strip()]
                        date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                        filed = ""
                        if date_match:
                            try: filed = datetime.strptime(date_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                            except: pass
                        link = await row.query_selector("a")
                        clerk_url = ""
                        if link:
                            href = await link.get_attribute("href") or ""
                            clerk_url = href if href.startswith("http") else f"{CLERK_BASE}{href}" if href else ""
                        if filed or (cells and len(cells[0]) > 3):
                            records.append({"doc_num":cells[0] if cells else "","doc_type":doc_code,"cat":doc_code,"cat_label":doc_label,"filed":filed,"owner":cells[2] if len(cells)>2 else "","grantee":cells[3] if len(cells)>3 else "","amount":0.0,"legal":"","clerk_url":clerk_url})
                            count += 1
                    except: continue
                if count: log.info(f"{doc_code}: {count} records")
                await asyncio.sleep(0.5)
            except Exception as e:
                log.warning(f"{doc_code}: {e}")
                continue

        await browser.close()

    all_records = records + api_records
    seen = set()
    unique = []
    for r in all_records:
        key = r.get("doc_num","") + r.get("filed","") + r.get("owner","")
        if key not in seen:
            seen.add(key)
            unique.append(r)
    log.info(f"Total unique: {len(unique)}")
    return unique

def build_output(records):
    enriched = []
    for rec in records:
        try:
            score, flags = compute_score(rec)
            rec["score"] = score
            rec["flags"] = flags
            rec.setdefault("prop_address",""); rec.setdefault("prop_city",""); rec.setdefault("prop_state","TX"); rec.setdefault("prop_zip","")
            rec.setdefault("mail_address",""); rec.setdefault("mail_city",""); rec.setdefault("mail_state","TX"); rec.setdefault("mail_zip","")
            enriched.append(rec)
        except: continue
    enriched.sort(key=lambda r: r.get("score",0), reverse=True)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    return {"fetched_at":datetime.now().isoformat(),"source":"Cameron County Clerk Portal","date_range":f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}","total":len(enriched),"with_address":sum(1 for r in enriched if r.get("prop_address")),"records":enriched}

def save_outputs(output):
    for d in OUTPUT_DIRS:
        Path(d).mkdir(parents=True, exist_ok=True)
        path = Path(d)/"records.json"
        with open(path,"w") as f: json.dump(output, f, indent=2, default=str)
        log.info(f"Saved {output['total']} records to {path}")

def export_ghl_csv(output):
    Path("data").mkdir(parents=True, exist_ok=True)
    fieldnames = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip","Property Address","Property City","Property State","Property Zip","Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with open("data/ghl_export.csv","w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in output.get("records",[]):
            owner = rec.get("owner","")
            parts = owner.replace(","," ").split()
            writer.writerow({"First Name":parts[1] if len(parts)>=2 else "","Last Name":parts[0] if parts else owner,"Mailing Address":rec.get("mail_address",""),"Mailing City":rec.get("mail_city",""),"Mailing State":rec.get("mail_state","TX"),"Mailing Zip":rec.get("mail_zip",""),"Property Address":rec.get("prop_address",""),"Property City":rec.get("prop_city",""),"Property State":rec.get("prop_state","TX"),"Property Zip":rec.get("prop_zip",""),"Lead Type":rec.get("cat_label",""),"Document Type":rec.get("doc_type",""),"Date Filed":rec.get("filed",""),"Document Number":rec.get("doc_num",""),"Amount/Debt Owed":rec.get("amount",""),"Seller Score":rec.get("score",0),"Motivated Seller Flags":", ".join(rec.get("flags",[])),"Source":"Cameron County Clerk","Public Records URL":rec.get("clerk_url","")})
    log.info("GHL CSV exported")

def main():
    log.info("=== Cameron County Motivated Seller Scraper ===")
    records = asyncio.run(scrape_with_playwright(LOOKBACK_DAYS))
    output = build_output(records)
    save_outputs(output)
    export_ghl_csv(output)
    log.info(f"=== Done: {output['total']} records ===")

if __name__ == "__main__":
    main()
