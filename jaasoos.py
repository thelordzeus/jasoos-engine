import csv
import io
import os
import re
import sys
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# ENCODING (Windows)
# =============================================================================
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# =============================================================================
# CONFIG
# =============================================================================

API_KEY = os.getenv(
    "SERPAPI_KEY",
    "PASTE_YOUR_KEY_HERE",
)   # REQUIRED in prod/Lambda
if not API_KEY:
    print("⚠ SERPAPI_KEY not set in environment. Set it before running.", flush=True)

PRIMARY_SITES = ["myntra", "slikk"]

SHOPPING_SITES = {
    "myntra": ["myntra.com"],
    "slikk": ["slikk.club"],
    "asian": ["asianfootwears.com"],
    "atom": [],
    "avant": ["avantgardeoriginal.com"],
    "bad_and_boujee": ["badandboujee.in"],
    "bauge": ["baugebags.com"],
    "beeglee": ["beeglee.in"],
    "bersache": ["bersache.com"],
    "bewakoof": ["bewakoof.com"],
    "blackberrys": ["blackberrys.com"],
    "bummer": ["bummer.in"],
    "campus_sutra": ["campussutra.com"],
    "chapter_2": ["chapter2drip.com"],
    "chumbak": ["chumbak.com"],
    "chupps": ["chupps.com"],
    "color_capital": ["colorcapital.in"],
    "crazybee": ["mavinclub.com"],
    "cult": [],
    "ecoright": ["ecoright.com"],
    "freehand": [],
    "guns_and_sons": ["gunsnsons.com"],
    "haute_sauce": ["buyhautesauce.com"],
    "highlander": [],
    "indian_garage_co": ["tigc.in"],
    "jar_gold": [],
    "jockey": ["jockey.in"],
    "just_lil_things": ["justlilthings.in"],
    "kedias": [],
    "lancer": [],
    "levis": ["levi.in"],
    "locomotive": [],
    "main_character": ["maincharacterindia.com"],
    "minute_mirth": [],
    "mydesignation": ["mydesignation.com"],
    "mywishbag": ["mywishbag.com"],
    "nailinit": ["nailin.it"],
    "palmonas": ["palmonas.com"],
    "pinacolada": ["buypinacolada.com"],
    "puma": ["in.puma.com"],
    "qissa": ["shopqissa.com"],
    "rapidbox": ["rapidbox.in"],
    "recast": ["recast.co.in"],
    "salty": ["salty.co.in"],
    "sassafras": ["sassafras.in"],
    "silisoul": ["silisoul.com"],
    "styli": ["stylishop.com", "styli.in"],
    "the_bear_house": ["thebearhouse.com", "bearhouseindia.com", "thebearhouse.in"],
    "the_indian_garage_co": ["tigc.in"],
    "the_kurta": ["thekurtacompany.com"],
    "theater": ["theater.xyz"],
    "thela_gaadi": ["thelagaadi.com"],
    "the_souled_store": ["thesouledstore.com"],
    "tokyo_talkies": [],
    "untung": ["untung.in"],
    "vara_vishudh": [],
    "vishudh": [],
    "xyxx": ["xyxxcrew.com"],
    "mascln_sassafras": ["sassafras.in"],
    "shae_by_sassafras": ["sassafras.in"],
    "pink_paprika_by_sassafras": ["sassafras.in"],
    "sassafras_basics": ["sassafras.in"],
    "sassafras_worklyf": ["sassafras.in"],
    "bearhouse": ["thebearhouse.com", "bearhouseindia.com", "thebearhouse.in"],
    "bearcompany": ["bearcompany.in", "thebearcompany.com"],
    "aatmana": ["akshahandmadejewelry.com"],
    "technosport": ["technosport.in"],
    "veirdo": ["veirdo.in"],
}

# =============================================================================
# RATE LIMITER (≤7 req/sec and ≤1000 req/hour)
# =============================================================================

class DualWindowRateLimiter:
    def __init__(self, per_sec=7, per_hour=1000):
        self.per_sec = per_sec
        self.per_hour = per_hour
        self.lock = threading.Lock()
        self.sec_window = deque()
        self.hour_window = deque()

    def acquire(self):
        # Block until both windows allow another request
        while True:
            now = time.time()
            with self.lock:
                # Clean old timestamps
                one_sec_ago = now - 1.0
                one_hour_ago = now - 3600.0

                while self.sec_window and self.sec_window[0] <= one_sec_ago:
                    self.sec_window.popleft()
                while self.hour_window and self.hour_window[0] <= one_hour_ago:
                    self.hour_window.popleft()

                if len(self.sec_window) < self.per_sec and len(self.hour_window) < self.per_hour:
                    self.sec_window.append(now)
                    self.hour_window.append(now)
                    return  # allowed

                # Compute sleep needed
                sleep_candidates = []
                if self.sec_window:
                    sleep_candidates.append(self.sec_window[0] + 1.0 - now)
                if self.hour_window:
                    sleep_candidates.append(self.hour_window[0] + 3600.0 - now)
                sleep_for = max(0.001, min(sleep_candidates) if sleep_candidates else 0.01)
            time.sleep(sleep_for)

# Shared limiter across all threads
RATE_LIMITER = DualWindowRateLimiter(per_sec=7, per_hour=1000)

API_CALL_COUNT = 0
API_CALL_LOCK = threading.Lock()

def _inc_api_call_count():
    global API_CALL_COUNT
    with API_CALL_LOCK:
        API_CALL_COUNT += 1

# =============================================================================
# HTTP SESSION (retry + timeouts)
# =============================================================================

def build_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"Accept": "application/json", "User-Agent": "serpapi-batcher/1.0"})
    return s

SESSION = build_session()

# =============================================================================
# HELPERS
# =============================================================================

def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (ValueError, TypeError):
        return default

def serpapi_get(params):
    """SERPAPI GET with global rate limiting + timeout."""
    if not API_KEY:
        return None
    RATE_LIMITER.acquire()
    try:
        _inc_api_call_count()
        resp = SESSION.get("https://serpapi.com/search", params=params, timeout=20)
        if resp.status_code == 200:
            return resp.json()
        return None
    except requests.RequestException:
        return None

def get_brand_site(brand_name: str):
    if not brand_name:
        return None
    brand_lower = str(brand_name).lower().replace(" ", "").replace("-", "").replace("_", "")
    brand_mapping = {
        "asian":"asian","asianfootwear":"asian","asianfootwears":"asian",
        "atom":"atom",
        "avant":"avant","avantgarde":"avant","avantgardeoriginal":"avant",
        "badboujee":"bad_and_boujee","badandboujee":"bad_and_boujee","bad&boujee":"bad_and_boujee",
        "bauge":"bauge","baugebags":"bauge",
        "beeglee":"beeglee",
        "bersache":"bersache",
        "bewakoof":"bewakoof","bwkf":"bewakoof",
        "blackberrys":"blackberrys","blackberry":"blackberrys",
        "bummer":"bummer",
        "campussutra":"campus_sutra","campus":"campus_sutra",
        "chapter2":"chapter_2","chaptertwo":"chapter_2",
        "chumbak":"chumbak",
        "chupps":"chupps",
        "colorcapital":"color_capital",
        "crazybee":"crazybee","mavinclub":"crazybee","mavin":"crazybee",
        "cult":"cult",
        "ecoright":"ecoright","ecoright":"ecoright","eco right":"ecoright",
        "freehand":"freehand","free hand":"freehand",
        "gunsandsons":"guns_and_sons","gunssons":"guns_and_sons","gunsnsons":"guns_and_sons","guns&sons":"guns_and_sons",
        "hautesauce":"haute_sauce",
        "highlander":"highlander",
        "jargold":"jar_gold",
        "jockey":"jockey",
        "justlilthings":"just_lil_things","justlilthings ":"just_lil_things","just lil things":"just_lil_things",
        "kedias":"kedias",
        "lancer":"lancer",
        "levis":"levis","levi":"levis","levi's":"levis",
        "locomotive":"locomotive",
        "maincharacter":"main_character",
        "minutemirth":"minute_mirth",
        "mydesignation":"mydesignation","designation":"mydesignation",
        "mywishbag":"mywishbag",
        "nailinit":"nailinit","nail in it":"nailinit",
        "palmonas":"palmonas",
        "pinacolada":"pinacolada",
        "puma":"puma",
        "qissa":"qissa",
        "rapidbox":"rapidbox","rapid box":"rapidbox",
        "recast":"recast",
        "salty":"salty",
        "sassafras":"sassafras","sassafrasbasics":"sassafras","sassafrasworklyf":"sassafras",
        "masclnsassafras":"sassafras","mascln":"sassafras","masclnbysassafras":"sassafras",
        "shaebysassafras":"sassafras","shae":"sassafras","shaesassafras":"sassafras",
        "pinkpaprikabysassafras":"sassafras","pinkpaprika":"sassafras","pinkpaprikasassafras":"sassafras",
        "silisoul":"silisoul",
        "styli":"styli","stylishop":"styli",
        "bearhouse":"the_bear_house","thebearhouse":"the_bear_house","bearhouseindia":"the_bear_house","thebearhouseindia":"the_bear_house",
        "bearcompany":"bearcompany","thebearcompany":"bearcompany","bearco":"bearcompany",
        "indiangarageco":"indian_garage_co","indiangaragecompany":"indian_garage_co","theindiangaragecompany":"indian_garage_co",
        "theindiangaragecom":"indian_garage_co","theindiangarageco":"indian_garage_co","theindiangarage":"indian_garage_co","tigc":"indian_garage_co",
        "thekurta":"the_kurta","thekurtacompany":"the_kurta","kurta":"the_kurta",
        "theater":"theater","theatre":"theater",
        "thelagaadi":"thela_gaadi","thela gaadi":"thela_gaadi",
        "thesouledstore":"the_souled_store","souledstore":"the_souled_store","tss":"the_souled_store",
        "tokyotalkies":"tokyo_talkies",
        "untung":"untung",
        "varabyvishudh":"vara_vishudh","vara":"vara_vishudh","vishudh":"vishudh",
        "xyxx":"xyxx","xyxxcrew":"xyxx",
        "aatmana": "aatmana",
        "technosport": "technosport",
        "veirdo": "veirdo",
        "levis": "levis", "levi": "levis", "levi's": "levis",
    }
    brand_site = brand_mapping.get(brand_lower)
    if brand_site and brand_site in SHOPPING_SITES:
        return brand_site
    if any(k in brand_lower for k in ["sassafras","mascln","shae","paprika"]):
        return "sassafras" if "sassafras" in SHOPPING_SITES else None
    return None

def extract_domain(url):
    try:
        parsed = urlparse(str(url).lower())
        return parsed.netloc.replace("www.", "")
    except Exception:
        return ""

def identify_site(url):
    domain = extract_domain(url)
    url_lower = str(url).lower()
    for site_key, site_patterns in SHOPPING_SITES.items():
        for pattern in site_patterns:
            if pattern in domain or pattern in url_lower:
                return site_key
    return None

def extract_price_from_match(match_data):
    price_info = match_data.get("price", {})
    if isinstance(price_info, dict):
        val = price_info.get("value", "")
        if val and val not in ["N/A", "", "null"]:
            cleaned = re.sub(r"[₹Rs.,\s*INR]", "", val, flags=re.IGNORECASE)
            if cleaned and cleaned.replace(".", "").isdigit():
                return cleaned
        extracted = price_info.get("extracted_value", "")
        if extracted and str(extracted) not in ["N/A", "", "null"]:
            return str(extracted)
    elif isinstance(price_info, str):
        if price_info and price_info not in ["N/A", "", "null"]:
            cleaned = re.sub(r"[₹Rs.,\s*INR]", "", price_info, flags=re.IGNORECASE)
            if cleaned and cleaned.replace(".", "").isdigit():
                return cleaned
    return "Price not displayed in listing"

def extract_colors_from_title(title):
    colors = ["black","white","blue","red","green","yellow","pink","purple","orange","brown","grey","gray","beige",
              "navy","olive","maroon","silver","gold","cream","khaki","tan","teal","burgundy","mint","lavender",
              "coral","peach","mustard","charcoal","rose"]
    title_lower = str(title).lower()
    return [c for c in colors if c in title_lower]

def check_brand_relaxed_match(match, target_brand, site_key):
    title = match.get("title", "").lower()
    link = match.get("link", "").lower()
    source = match.get("source", "").lower()
    if site_key and site_key not in ["myntra", "slikk"]:
        return True
    if not target_brand:
        return False
    target_lower = target_brand.lower()
    brand_keywords = target_lower.replace("-", " ").replace("_", " ").split()
    variations = {
        target_lower, target_lower.replace(" ", ""), target_lower.replace(" ", "-"), target_lower.replace(" ", "_")
    }
    if len(brand_keywords) > 1:
        without_the = " ".join([w for w in brand_keywords if w != "the"]) or target_lower
        variations |= {without_the, without_the.replace(" ", "")}
    # Brand-specific shortcuts
    alias_map = {
        "asian": ["asian", "asian footwear", "asianfootwears"],
        "avant": ["avant", "avant garde", "avantgarde"],
        "bad boujee": ["bad boujee", "bad and boujee", "bad & boujee"],
        "bewakoof": ["bewakoof","bwkf"],
        "blackberry": ["blackberrys","blackberry"],
        "campus": ["campussutra","campus sutra","campus"],
        "chapter": ["chapter2","chapter 2","chapter two"],
        "crazybee": ["crazybee","mavin","mavinclub"],
        "ecoright": ["ecoright","eco right"],
        "freehand": ["freehand","free hand"],
        "guns": ["guns","sons","gunsnsons","guns & sons","guns and sons"],
        "levis": ["levis","levi","levi's"],
        "main character": ["main character","maincharacter"],
        "mydesignation": ["mydesignation","my designation","designation"],
        "mywishbag": ["mywishbag","my wish bag"],
        "nailinit": ["nailinit","nail in it"],
        "pinacolada": ["pinacolada","pina colada"],
        "rapidbox": ["rapidbox","rapid box"],
        "sassafras": ["sassafras","mascln","shae","pink paprika","sassafras basics","sassafras worklyf"],
        "styli": ["styli","stylishop"],
        "bear": ["bear","bearhouse","bear house","thebearhouse","the bear house","bearcompany","bear company","thebearcompany","the bear company"],
        "indian garage": ["indiangarage","indian garage","tigc","the indian garage co"],
        "kurta": ["kurta","the kurta","the kurta company"],
        "theater": ["theater","theatre"],
        "thela": ["thela gaadi","thelagaadi"],
        "souled store": ["souled store", "the souled store", "tss", "the souled store official"],
        "vara": ["vara","vishudh","vara by vishudh"],
        "xyxx": ["xyxx","xyxx crew"],
        "aatmana": ["aatmana", "aksha handmade jewelry", "aksha"],
        "technosport": ["technosport"],
        "veirdo": ["veirdo"],
    }
    for k, vals in alias_map.items():
        if k in target_lower:
            variations |= set(vals)
    combined = f"{title} {link} {source}"
    return any(v for v in variations if len(v) > 2 and v in combined)

def is_valid_product_url(url):
    url_lower = str(url).lower()
    invalid_patterns = [
        "/collections/","/collection/","/category/","/categories/","/search","?search=","/s?","/find/","/brand/","/brands/",
        "/sale/","/deals/","/all-products","/shop?","/filter","/sort=","?page=","&page=","/men/","/women/","/kids/","/unisex/",
        "/clothing/","/accessories/","/footwear/",
    ]
    if any(p in url_lower for p in invalid_patterns):
        return False
    # Site-specific checks (kept from original)
    if "myntra.com" in url_lower: return "/buy" in url_lower or "/p/" in url_lower
    if "slikk.club" in url_lower: return True
    mapping_checks = [
        ("asianfootwears.com", ["/products/","/product/"]),
        ("avantgardeoriginal.com", ["/products/","/product/"]),
        ("badandboujee.in", ["/products/","/product/"]),
        ("baugebags.com", ["/products/","/product/"]),
        ("beeglee.in", ["/products/","/product/"]),
        ("bersache.com", ["/products/","/product/"]),
        ("bewakoof.com", ["/p/","/product/","/buy"]),
        ("blackberrys.com", ["/products/","/product/"]),
        ("bummer.in", ["/products/","/product/"]),
        ("campussutra.com", ["/products/","/product/"]),
        ("chapter2drip.com", ["/products/","/product/"]),
        ("chumbak.com", ["/products/","/product/"]),
        ("chupps.com", ["/products/","/product/"]),
        ("colorcapital.in", ["/products/","/product/"]),
        ("mavinclub.com", ["/products/","/product/"]),
        ("ecoright.com", ["/products/","/product/"]),
        ("gunsnsons.com", ["/products/","/product/"]),
        ("buyhautesauce.com", ["/products/","/product/"]),
        ("jockey.in", ["/products/","/product/"]),
        ("justlilthings.in", ["/products/","/product/"]),
        ("levi.in", ["/products/","/product/","/in-en/p/"]),
        ("maincharacterindia.com", ["/products/","/product/"]),
        ("mydesignation.com", ["/products/"]),
        ("mywishbag.com", ["/products/","/product/"]),
        ("nailin.it", ["/products/","/product/"]),
        ("palmonas.com", ["/products/","/product/"]),
        ("buypinacolada.com", ["/products/","/product/"]),
        ("puma.com", ["/products/","/product/","/in/en/"]),
        ("in.puma.com", ["/products/","/product/","/in/en/"]),
        ("shopqissa.com", ["/products/","/product/"]),
        ("rapidbox.in", ["/products/","/product/"]),
        ("recast.co.in", ["/products/","/product/"]),
        ("salty.co.in", ["/products/","/product/"]),
        ("sassafras.in", ["/products/"]),
        ("silisoul.com", ["/products/","/product/"]),
        ("stylishop.com", ["/products/","/product/"]),
        ("styli.in", ["/products/","/product/"]),
        ("thebearhouse", ["/products/","/product/"]),
        ("bearcompany", ["/products/","/product/"]),
        ("tigc.in", ["/products/"]),
        ("thekurtacompany.com", ["/products/","/product/"]),
        ("thesouledstore.com", ["/products/","/product/"]),
        ("theater.xyz", ["/products/","/product/"]),
        ("thelagaadi.com", ["/products/","/product/"]),
        ("untung.in", ["/products/","/product/"]),
        ("xyxxcrew.com", ["/products/","/product/"]),
        ("akshahandmadejewelry.com", ["/products/","/product/"]),
        ("technosport.in", ["/products/","/product/"]),
        ("veirdo.in", ["/products/","/product/"]),
    ]
    for host, must in mapping_checks:
        if host in url_lower:
            return any(m in url_lower for m in must)
    path_segments = [s for s in url_lower.split("/") if s and not s.startswith("?")]
    return len(path_segments) >= 3

def calculate_title_similarity(original_title, found_title):
    if not original_title or not found_title:
        return 0
    orig_lower = str(original_title).lower()
    found_lower = str(found_title).lower()
    stop_words = {"the","a","an","and","or","but","with","for","on","in","at","to","buy","shop","online"}
    orig_keywords = set(re.findall(r"\b\w+\b", orig_lower)) - stop_words
    found_keywords = set(re.findall(r"\b\w+\b", found_lower)) - stop_words
    if not orig_keywords:
        return 0
    common_keywords = orig_keywords & found_keywords
    overlap = (len(common_keywords) / len(orig_keywords)) * 100
    orig_colors = extract_colors_from_title(orig_lower)
    found_colors = extract_colors_from_title(found_lower)
    color_bonus = 15 if orig_colors and any(c in found_colors for c in orig_colors) else ( -20 if orig_colors and found_colors else 0 )
    return max(0, min(100, overlap + color_bonus))

def extract_product_info(visual_matches, target_brand, allowed_sites, original_title="", pass_type="first"):
    results = {site_key: {"url": "Not Found", "price": "Product not available on site"} for site_key in allowed_sites}
    if not visual_matches:
        return results

    candidates = {site_key: [] for site_key in allowed_sites}
    for idx, match in enumerate(visual_matches, 1):
        link = match.get("link", "")
        if not link:
            continue
        site_key = identify_site(link)
        if not site_key or site_key not in allowed_sites:
            continue
        if not check_brand_relaxed_match(match, target_brand, site_key):
            continue
        if not is_valid_product_url(link):
            continue
        similarity = calculate_title_similarity(original_title, match.get("title", ""))
        is_marketplace = site_key in ["myntra", "slikk"]
        threshold = 5 if is_marketplace else 15
        if similarity < threshold:
            continue
        candidates[site_key].append({
            "url": link,
            "price": extract_price_from_match(match),
            "visual_rank": idx,
            "similarity": similarity,
        })

    for site_key in allowed_sites:
        if not candidates[site_key]:
            continue
        is_marketplace = site_key in ["myntra", "slikk"]
        best = min(candidates[site_key], key=lambda x: x["visual_rank"]) if is_marketplace else \
               max(candidates[site_key], key=lambda x: x["similarity"] - (x["visual_rank"] * 5))
        results[site_key] = {"url": best["url"], "price": best["price"]}
    return results

def search_image_on_serpapi(image_url):
    params = {
        "engine": "google_lens",
        "url": image_url,
        "api_key": API_KEY,
        "country": "in",
        "hl": "en",
        "no_cache": "false",
    }
    return serpapi_get(params)

def search_image_with_query_on_serpapi(image_url, query_text):
    params = {
        "engine": "google_lens",
        "url": image_url,
        "q": query_text,
        "api_key": API_KEY,
        "country": "in",
        "hl": "en",
        "no_cache": "false",
    }
    return serpapi_get(params)

# =============================================================================
# CORE PER-PRODUCT
# =============================================================================

def process_single_product_pass1(product, product_idx, total_products):
    """PASS 1: pure visual search only."""
    t0 = time.time()
    title = product.get("product_title", "") or ""
    brand = product.get("brand", "") or ""
    image_url = product.get("image", "") or ""

    if not image_url:
        print(f"[PASS 1] [{product_idx}/{total_products}] ⚠ No image URL → skipped", flush=True)
        return {
            "product": product,
            "site_results": {},
            "brand_site": None,
            "allowed_sites": [],
        }

    brand_site = get_brand_site(brand)
    allowed_sites = PRIMARY_SITES.copy()
    if brand_site:
        allowed_sites.append(brand_site)

    site_results = {
        s: {"url": "Not Found", "price": "Product not available on site"}
        for s in allowed_sites
    }

    search_results = search_image_on_serpapi(image_url)
    if search_results:
        visual_matches = search_results.get("visual_matches", []) or []
        extracted = extract_product_info(
            visual_matches, brand, allowed_sites, title, "first"
        )
        for k, v in extracted.items():
            if v["url"] != "Not Found":
                site_results[k] = v

    found_count = sum(1 for v in site_results.values() if v["url"] != "Not Found")
    elapsed = time.time() - t0
    ptitle = (title[:48] + "…") if len(title) > 49 else title
    print(
        f"[PASS 1] [{product_idx}/{total_products}] ✅ {found_count}/{len(allowed_sites)} sites | {ptitle} | {elapsed:.2f}s",
        flush=True,
    )

    return {
        "product": product,
        "site_results": site_results,
        "brand_site": brand_site,
        "allowed_sites": allowed_sites,
    }


def process_single_product_pass2(result_entry, product_idx, total_products):
    """PASS 2: site-specific queries for missing results."""
    t0 = time.time()
    product = result_entry["product"]
    site_results = result_entry["site_results"]
    allowed_sites = result_entry.get("allowed_sites", [])

    title = product.get("product_title", "") or ""
    brand = product.get("brand", "") or ""
    image_url = product.get("image", "") or ""

    if not image_url or not allowed_sites:
        return result_entry

    missing_sites = [
        s for s in allowed_sites if site_results.get(s, {}).get("url") == "Not Found"
    ]
    if not missing_sites:
        return result_entry

    brand_for_query = brand
    if brand and any(x in brand.lower() for x in ["mascln", "shae", "pink paprika"]):
        brand_for_query = "SASSAFRAS"

    ptitle = (title[:48] + "…") if len(title) > 49 else title
    print(
        f"[PASS 2] [{product_idx}/{total_products}] 🔄 {len(missing_sites)} missing | {ptitle}",
        flush=True,
    )

    for missing_site in missing_sites:
        site_domain = None
        if missing_site == "myntra":
            site_domain = "myntra.com"
        elif missing_site == "slikk":
            site_domain = "slikk.club"
        else:
            domains = SHOPPING_SITES.get(missing_site, [])
            if domains:
                site_domain = domains[0]

        if not site_domain:
            continue

        query = (
            f"{brand_for_query} site:{site_domain}"
            if brand_for_query
            else f"site:{site_domain}"
        )
        search_results = search_image_with_query_on_serpapi(product.get("image", ""), query)
        if not search_results:
            continue

        visual_matches = search_results.get("visual_matches", []) or []
        pass2_updates = extract_product_info(
            visual_matches, brand, [missing_site], title, "second"
        )
        if pass2_updates.get(missing_site, {}).get("url") != "Not Found":
            site_results[missing_site] = pass2_updates[missing_site]

    found_count = sum(1 for v in site_results.values() if v["url"] != "Not Found")
    elapsed = time.time() - t0
    print(
        f"[PASS 2] [{product_idx}/{total_products}] ✅ {found_count}/{len(allowed_sites)} sites | {ptitle} | {elapsed:.2f}s",
        flush=True,
    )

    result_entry["site_results"] = site_results
    return result_entry

# =============================================================================
# PIPELINE
# =============================================================================

def process_products(input_csv, output_csv, batch_size=8, max_workers=8):
    start_ts = time.time()

    # Read CSV (same schema)
    products = []
    try:
        with open(input_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append({
                    "style_id": row.get("style_id", "") or "",
                    "brand": row.get("brand_name", "") or "",
                    "product_title": row.get("product_title", "") or "",
                    "gender": row.get("gender", "") or "",
                    "category": row.get("category", "") or "",
                    "min_price_rupees": row.get("min_price_rupees", "") or "",
                    "image": row.get("first_image_url", "") or "",
                    "view_count": safe_int(row.get("view_count", 0), 0),
                })
    except FileNotFoundError:
        print(f"❌ Input file not found: {input_csv}", flush=True)
        return
    except Exception as e:
        print(f"❌ Error reading CSV: {e}", flush=True)
        return

    if not products:
        print("⚠ No products to process.", flush=True)
        return

    total = len(products)
    print("=" * 80, flush=True)
    print(f"🔎 SERPAPI BATCHER v2.0 | Items: {total}", flush=True)
    print("✅ PHASE 1: Pure visual search for all products", flush=True)
    print('✅ PHASE 2: Site-specific queries only for missing URLs', flush=True)
    print(f"Rate limits → ≤7 req/sec AND ≤1000 req/hour (enforced)", flush=True)
    print("=" * 80, flush=True)

    # PASS 1: run pure visual search for everyone
    pass1_results = []
    idx = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, product in enumerate(products, 1):
            fut = executor.submit(process_single_product_pass1, product, i, total)
            futures[fut] = i
            idx += 1
            if idx % batch_size == 0:
                for _ in range(batch_size):
                    done = next(as_completed(futures), None)
                    if not done:
                        break
                    try:
                        res = done.result()
                        pass1_results.append(res)
                    except Exception as e:
                        print(f"❌ Pass 1 worker error: {e}", flush=True)
                    del futures[done]
        for done in as_completed(futures):
            try:
                res = done.result()
                pass1_results.append(res)
            except Exception as e:
                print(f"❌ Pass 1 worker error: {e}", flush=True)

    print("=" * 80, flush=True)
    print(f"✅ PHASE 1 COMPLETE: {len(pass1_results)} products processed", flush=True)

    # Determine items needing Pass 2
    products_needing_pass2 = [
        entry
        for entry in pass1_results
        if any(
            entry.get("site_results", {}).get(site, {}).get("url") == "Not Found"
            for site in entry.get("allowed_sites", [])
        )
    ]

    if products_needing_pass2:
        print("=" * 80, flush=True)
        print(f"🚀 PHASE 2: Running site-specific queries for {len(products_needing_pass2)} products", flush=True)
        pass2_results = []
        idx = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, entry in enumerate(products_needing_pass2, 1):
                fut = executor.submit(process_single_product_pass2, entry, i, len(products_needing_pass2))
                futures[fut] = i
                idx += 1
                if idx % batch_size == 0:
                    for _ in range(batch_size):
                        done = next(as_completed(futures), None)
                        if not done:
                            break
                        try:
                            res = done.result()
                            pass2_results.append(res)
                        except Exception as e:
                            print(f"❌ Pass 2 worker error: {e}", flush=True)
                        del futures[done]
            for done in as_completed(futures):
                try:
                    res = done.result()
                    pass2_results.append(res)
                except Exception as e:
                    print(f"❌ Pass 2 worker error: {e}", flush=True)

        # Merge Pass 2 results back into pass1_results
        pass2_by_style = {r["product"].get("style_id"): r for r in pass2_results}
        for idx, entry in enumerate(pass1_results):
            style_id = entry["product"].get("style_id")
            if style_id in pass2_by_style:
                pass1_results[idx] = pass2_by_style[style_id]

        print("=" * 80, flush=True)
        print(f"✅ PHASE 2 COMPLETE: {len(pass2_results)} products reprocessed", flush=True)
    else:
        print("=" * 80, flush=True)
        print("✅ PHASE 2 SKIPPED: All URLs resolved in Pass 1", flush=True)

    all_results = pass1_results

    # Sort by view_count desc (as before)
    all_results.sort(key=lambda x: x.get("product", {}).get("view_count", 0), reverse=True)
    print(f"✅ Sorted {len(all_results)} products by view_count", flush=True)

    # Write CSV
    fieldnames = [
        "view_count","style_id","brand","product_title","gender","category",
        "myntra_price","slikk_price","brand_price",
        "myntra_url","slikk_url","brand_url",
    ]
    try:
        with open(output_csv, "w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for result_entry in all_results:
                product = result_entry.get("product", {}) or {}
                site_results = result_entry.get("site_results", {}) or {}
                brand_site = result_entry.get("brand_site")

                myntra_data = site_results.get("myntra", {"url": "Not Found", "price": "Product not available on site"})
                slikk_data = site_results.get("slikk", {"url": "Not Found", "price": "Product not available on site"})
                if brand_site:
                    brand_data = site_results.get(brand_site, {"url": "Not Found", "price": "Product not available on site"})
                else:
                    brand_data = {"url": "Not Found", "price": "Product not available on site"}

                style_id = product.get("style_id", "") or ""
                writer.writerow({
                    "view_count": product.get("view_count", 0),
                    "style_id": style_id,
                    "brand": product.get("brand", "") or "",
                    "product_title": product.get("product_title", "") or "",
                    "gender": product.get("gender", "") or "",
                    "category": product.get("category", "") or "",
                    "myntra_price": myntra_data.get("price", ""),
                    "slikk_price": slikk_data.get("price", ""),
                    "brand_price": brand_data.get("price", ""),
                    "myntra_url": myntra_data.get("url", ""),
                    "slikk_url": slikk_data.get("url", ""),
                    "brand_url": brand_data.get("url", ""),
                })
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}", flush=True)
        return

    # Coverage summary (short)
    total_products = len(all_results) or 1
    myntra_count = sum(1 for r in all_results if r.get("site_results", {}).get("myntra", {}).get("url") != "Not Found")
    slikk_count  = sum(1 for r in all_results if r.get("site_results", {}).get("slikk", {}).get("url") != "Not Found")
    brand_count  = 0
    for r in all_results:
        bs = r.get("brand_site")
        if bs and r.get("site_results", {}).get(bs, {}).get("url") != "Not Found":
            brand_count += 1

    end_ts = time.time()
    elapsed = end_ts - start_ts
    print("=" * 80, flush=True)
    print(f"MYNTRA: {myntra_count}/{total_products} | SLIKK: {slikk_count}/{total_products} | BRAND: {brand_count}/{total_products}", flush=True)
    print(f"⏱ Total time: {elapsed:.2f}s for {total_products} products", flush=True)
    print(f"📄 Output: {output_csv}", flush=True)
    print(f"📡 Total SERPAPI calls: {API_CALL_COUNT}", flush=True)
    print("=" * 80, flush=True)

# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    INPUT_FILE = "bew.csv"
    OUTPUT_FILE = "bewa.csv"
    try:
        process_products(INPUT_FILE, OUTPUT_FILE, batch_size=8, max_workers=8)
        print("✅ ALL DONE", flush=True)
    except KeyboardInterrupt:
        print("⚠️ Interrupted by user.", flush=True)
    except Exception as e:
        print(f"❌ Fatal error: {e}", flush=True)
