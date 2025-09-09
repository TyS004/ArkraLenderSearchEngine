import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import json
import re
from google import genai
import os

# === Gemini Setup ===
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise EnvironmentError("GEMINI_API_KEY not set in .env or environment")
os.environ["GEMINI_API_KEY"] = api_key

client = genai.Client()

# === Prompt-based AI Parsing ===
def parse_with_gemini(raw_text, url=None):
    prompt = f"""
You are a data extraction AI working for a commercial lending search engine. 
Extract structured lending information from the following scraped website text.

Return your response in **JSON** with these fields:
- Lender Name
- Lender Type (Bank, Credit Union, Independent Lender, Vendor Financing)
- General Product Types
- Finance Reasons (Inventory, Expansion, Debt Consolidation, Working Capital, Equipment Purchase, Refinance, N/A)
- Sub-Product Type (Unsecured Term Loans, SBA, Equipment Loans, etc.)
- Loan Amount (min and max if available)
- Starting Rate
- Term Length
- Origination Fee
- Min Time in Business
- Min Revenue
- FICO Score
- Online Application Status (Self-Serve or Consultation Required)
- Product Page URL
- Geography Served
- Veteran Programs
- Minority Programs
- Notes (any important details)
- As-of Date (use today's date)

Scraped content:
\"\"\" 
{raw_text.strip()[:10000]} 
\"\"\" 

URL: {url or 'N/A'}

Only respond with a JSON object.
    """
    try:
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        text = response.text.strip()
        json_str = text
        if not text.startswith('{'):
            start = text.find('{')
            end = text.rfind('}') + 1
            json_str = text[start:end]
        return json.loads(json_str)
    except Exception as e:
        print("❌ Error parsing with Gemini:", e)
        return None

# === Manual Fallback Parser ===
def fallback_parse_lender_data(text, lender, as_of):
    print(f"⚠️ Using fallback parser for: {lender['name']}")
    products = []

    product_keywords = {
        "Term Loans": ["term loan", "term financing"],
        "Unsecured Loans": ["unsecured loan", "no collateral"],
        "Equipment Loans & Leases": ["equipment financing", "equipment loan", "equipment lease"],
        "Lines of Credit": ["line of credit", "credit line"],
        "Corporate Credit Cards": ["business credit card", "corporate card"],
        "Venture Debt": ["venture debt"],
        "Vendor Financing": ["vendor financing", "dealer financing"]
    }

    for product, keywords in product_keywords.items():
        for keyword in keywords:
            if re.search(rf"\b{keyword}\b", text, re.IGNORECASE):
                products.append(product)
                break

    return {
        "Lender Name": lender["name"],
        "Lender Type": lender["type"],
        "General Product Types": ", ".join(products) if products else "N/A",
        "Finance Reasons": "N/A",
        "Sub-Product Type": "N/A",
        "Loan Amount": "N/A",
        "Starting Rate": "N/A",
        "Term Length": "N/A",
        "Origination Fee": "N/A",
        "Min Time in Business": "N/A",
        "Min Revenue": "N/A",
        "FICO Score": "N/A",
        "Online Application Status": "N/A",
        "Product Page URL": lender["url"],
        "Geography Served": "N/A",
        "Veteran Programs": "N/A",
        "Minority Programs": "N/A",
        "Notes": "Parsed with fallback parser",
        "As-of Date": as_of
    }

# === Lender List ===
LENDERS = [
    {"name": "36th Street Capital", "url": "https://36thstreetcapital.com/", "type": "Independent Lender"},
    {"name": "Alliance Funding Group", "url": "https://afg.com/", "type": "Independent Lender"},
    {"name": "Apple Bank", "url": "https://www.applebank.com/", "type": "Bank"},
    {"name": "Associated Bank Equipment Finance", "url": "https://www.associatedbank.com", "type": "Bank"},
    {"name": "Atlantic Union Equipment Finance", "url": "https://www.atlanticunionbank.com/", "type": "Bank"},
]

def log_failure(lender, reason):
    with open("scrape_errors.log", "a") as f:
        f.write(f"{lender['name']} | {lender['url']} | {reason}\n")

# === Playwright Scraper ===
from playwright.sync_api import sync_playwright

def get_page_html(url):
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        try:
            page.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "stylesheet", "font"] else route.continue_())
            page.goto(url, timeout=30000, wait_until="load")
            html = page.content()
        except Exception as e:
            print(f"❌ Playwright failed to load {url}: {e}")
            html = None
        finally:
            browser.close()
        return html

# === Main Scraper ===
def scrape():
    rows = []
    as_of = datetime.today().strftime('%Y-%m-%d')

    for lender in LENDERS:
        name = lender["name"]
        url = lender["url"]
        lender_type = lender["type"]

        print(f"🔍 Scraping: {name} ({url})")

        try:
            html = get_page_html(url)
            if not html:
                print(f"❌ Failed to fetch {url} - No HTML returned")
                log_failure(lender, "No HTML returned")
                continue

            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator=" ", strip=True)

            # Try AI first
            parsed = parse_with_gemini(text, url=url)

            # Fallback
            if parsed is None:
                parsed = fallback_parse_lender_data(text, lender, as_of)
                log_failure(lender, "Gemini failed, used fallback")

            # Append missing fields if needed
            parsed["As-of Date"] = as_of
            parsed["Lender Name"] = parsed.get("Lender Name") or lender["name"]
            if "Lender Type" not in parsed or parsed["Lender Type"] == "N/A":
                parsed["Lender Type"] = lender_type

            rows.append(parsed)

        except Exception as e:
            print(f"❌ Error scraping {name}: {e}")
            log_failure(lender, f"Exception: {e}")
            continue

    return rows

# === CSV Writer ===
def write_csv(rows, filename="lenders_output.csv"):
    if not rows:
        print("⚠️ No data to write.")
        return
    keys = rows[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ Wrote {len(rows)} lenders to {filename}")

# === Run ===
if __name__ == "__main__":
    data = scrape()
    write_csv(data)