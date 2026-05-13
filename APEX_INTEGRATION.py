# APEX Integration — Add this route to your routes/leads.py
# This allows the Cameron County scraper to push leads directly into APEX

# Add this import at the top of routes/leads.py:
# from flask import jsonify

# Add this route to routes/leads.py:

"""
@leads_bp.route('/api/import_lead', methods=['POST'])
def api_import_lead():
    data = request.json or {}
    owner = data.get('owner', '').strip()
    prop_addr = data.get('prop_addr', '').strip()
    mail_addr = data.get('mail_addr', '').strip()
    amount = data.get('amount', '').strip()
    county = data.get('county', 'Cameron').strip()
    source = data.get('source', 'Cameron County Clerk').strip()
    assessed = data.get('assessed', '').strip()

    if not owner or not prop_addr:
        return jsonify({'status': 'error', 'message': 'Missing owner or address'}), 400

    # Check if lead already exists
    import sqlite3
    conn = sqlite3.connect('apex.db')
    existing = conn.execute(
        'SELECT id FROM leads WHERE property_address=? AND owner_name=?',
        (prop_addr.upper(), owner.upper())
    ).fetchone()

    if existing:
        conn.close()
        return jsonify({'status': 'exists', 'id': existing[0]})

    conn.execute(
        'INSERT INTO leads (owner_name, property_address, mailing_address, amount_owed, county, source, date_added, assessed_value) VALUES (?,?,?,?,?,?,?,?)',
        (owner.upper(), prop_addr.upper(), (mail_addr or prop_addr).upper(), amount, county, source, str(__import__('datetime').date.today()), assessed or None)
    )
    conn.commit()
    lead_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    conn.close()

    return jsonify({'status': 'created', 'id': lead_id})
"""

# ── How to run the scraper ──────────────────────────────────────────────────

# 1. Install dependencies:
#    pip install requests beautifulsoup4 lxml playwright
#    python -m playwright install chromium

# 2. Run once manually:
#    python scraper/fetch.py

# 3. To push leads to APEX automatically, set environment variable:
#    PUSH_TO_APEX=true python scraper/fetch.py

# 4. Results saved to:
#    dashboard/records.json  — for GitHub Pages dashboard
#    data/records.json       — raw data
#    data/ghl_export.csv     — GHL/CRM import ready

# ── File Structure ──────────────────────────────────────────────────────────

# scraper/
#   fetch.py              — main scraper
#   requirements.txt      — dependencies
# dashboard/
#   index.html            — web dashboard (GitHub Pages)
#   records.json          — scraped data
# data/
#   records.json          — same data
#   ghl_export.csv        — GHL export
# .github/workflows/
#   scrape.yml            — daily GitHub Actions job

# ── GitHub Setup ───────────────────────────────────────────────────────────

# 1. Create a new GitHub repo
# 2. Upload all files
# 3. Enable GitHub Pages: Settings > Pages > Source: gh-pages branch
# 4. GitHub Actions will run daily at 7am UTC automatically
# 5. Your dashboard URL: https://YOUR_USERNAME.github.io/YOUR_REPO/
