"""
Universal Price Extractor - High Performance + Stable Slikk
Scrapes prices from Myntra, Slikk, and Brand URLs, optimized for large datasets.

Input schema (fixed):

    view_count,
    style_id,
    brand,
    product_title,
    gender,
    category,
    product_price,
    myntra_price,
    slikk_price,
    brand_price,
    product_url,
    myntra_url,
    slikk_url,
    brand_url

Input  file : wow.csv
Output file : pr.csv
"""

import csv
import requests
import re
from bs4 import BeautifulSoup
import sys
import io
import random
from typing import Optional, Dict, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
import time

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# -----------------------------
# CONFIG
# -----------------------------

# ScraperAPI configuration for Slikk (JavaScript rendering)
SCRAPERAPI_KEY = 'PASTE_YOUR_SCRAPERAPI_KEY_HERE'  # your key

# List of common user agents to rotate through
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
]

# Use ScraperAPI for Slikk even if price already present?
# False = only call API when slikk_price is missing / placeholder
FORCE_UPDATE_SLIKK = False

# Parallelism for network-bound work
MAX_WORKERS = 24  # tune as needed

INPUT_CSV = 'soulded-store-20jan.csv'
OUTPUT_CSV = 'soul-store20jan.csv'

EXPECTED_FIELDNAMES = [
    "view_count",
    "style_id",
    "brand",
    "product_title",
    "gender",
    "category",
    "product_price",
    "myntra_price",
    "slikk_price",
    "brand_price",
    "product_url",
    "myntra_url",
    "slikk_url",
    "brand_url",
]

# -----------------------------
# SLIKK STABILITY SETTINGS
# -----------------------------

# How many Slikk ScraperAPI calls can run at the same time
SLIKK_MAX_PARALLEL = 3   # try 2–3; lower = gentler, more reliable

# How many times to retry a Slikk call if it fails or returns tiny HTML
SLIKK_RETRIES = 3

# Base delay between retries (seconds). Backoff: 2s, 4s, 6s ...
SLIKK_RETRY_DELAY = 2.0

# Semaphore to limit parallel Slikk calls across all threads
slikk_semaphore = threading.BoundedSemaphore(SLIKK_MAX_PARALLEL)

# -----------------------------
# PRICE EXTRACTOR CLASS
# -----------------------------

class PriceExtractor:
    """Extract prices from Myntra, Slikk, and supported brand websites, high-performance."""

    def __init__(self, max_retries: int = 3, timeout: int = 30):
        self.max_retries = max_retries
        self.timeout = timeout

        # Supported brand domains (Shopify-based stores + others)
        self.supported_brands = [
            'tigc.in', 'bewakoof', 'sassafras', 'thebearhouse', 'bearhouse',
            'mydesignation', 'shopqissa', 'campussutra', 'beeglee',
            'colorcapital', 'maincharacter', 'theater', 'blackberrys',
            'pinacolada', 'buypinacolada', 'silisoul', 'veirdo.in', 'technosport.in',
            'thesouledstore.com', 'jockey.in', 'puma.com', 'asianfootwears.com',
            'avantgardeoriginal.com', 'badandboujee.in', 'baugebags.com',
            'bersache.com', 'bummer.in', 'chapter2drip.com', 'chumbak.com',
            'chupps.com', 'mavinclub.com', 'ecoright.com', 'gunsnsons.com',
            'buyhautesauce.com', 'justlilthings.in', 'levi.in',
            'maincharacterindia.com', 'mywishbag.com', 'nailin.it', 'palmonas.com',
            'rapidbox.in', 'recast.co.in', 'salty.co.in', 'stylishop.com', 'styli.in',
            'thekurtacompany.com', 'thelagaadi.com', 'untung.in', 'xyxxcrew.com',
            'akshahandmadejewelry.com'
        ]

        # Reusable HTTP session with connection pooling
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=0,  # we handle retries ourselves
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _get_random_user_agent(self) -> str:
        """Get a random user agent from the list."""
        return random.choice(USER_AGENTS)

    def get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if match:
            return match.group(1)
        return ''

    def is_brand_supported(self, url: str) -> bool:
        """Check if brand URL is supported for scraping."""
        if not url or url == 'Not Found':
            return False

        domain = self.get_domain(url).lower()
        return any(brand in domain for brand in self.supported_brands)

    # -------------------- GENERIC PRICE REGEX --------------------

    def extract_price_generic_regex(self, html_content: str) -> Optional[str]:
        """Try to extract price using generic regex patterns."""
        price_patterns = [
            r'"price"[:\s]+["\']?(\d+)[\.\d]*["\']?',
            r'"mrp"[:\s]+["\']?(\d+)[\.\d]*["\']?',
            r'₹\s*(\d[\d,]*(?:\.\d{2})?)',
            r'Rs\.?\s*(\d[\d,]*(?:\.\d{2})?)',
            r'INR\s*(\d[\d,]*(?:\.\d{2})?)',
            r'price["\s:]+(\d[\d,]+)',
        ]

        for pattern in price_patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                for match in matches:
                    cleaned = re.sub(r'[,\s]', '', str(match))
                    try:
                        price_val = float(cleaned)
                        if 50 <= price_val <= 100000:
                            return cleaned
                    except Exception:
                        continue

        return None

    # -------------------- MYNTRA --------------------

    def extract_myntra_price(self, soup: BeautifulSoup, html_content: str) -> Optional[str]:
        """Extract price from Myntra."""
        selectors = [
            {'class': 'pdp-price'},
            {'class': 'pdp-discount-container'},
            {'class': 'product-price'},
            {'class': 'price-value'}
        ]

        for selector in selectors:
            element = soup.find('span', selector)
            if element:
                price = element.get_text()
                cleaned = self.clean_price(price)
                if cleaned:
                    return cleaned

        script = soup.find('script', type='application/ld+json')
        if script and script.string:
            match = re.search(r'"price":\s*"?(\d+)"?', script.string)
            if match:
                return match.group(1)

        return self.extract_price_generic_regex(html_content)

    # -------------------- SLIKK URL NORMALIZATION --------------------

    def normalize_slikk_url(self, url: str) -> str:
        """
        Normalize Slikk URL:
        - If last segment is numeric ID (e.g., 61050603 or 351389607076),
          rewrite to: https://www.slikk.club/product/<ID>
        - Else: just URL-encode spaces in path.
        """
        from urllib.parse import urlparse, urlunparse, quote

        if not url:
            return url

        parsed = urlparse(url)
        path = parsed.path

        segments = [seg for seg in path.split('/') if seg]
        if segments:
            last = segments[-1]
            if last.isdigit() and len(last) >= 5:
                product_id = last
                return f"https://www.slikk.club/product/{product_id}"

        encoded_path = quote(path, safe='/')
        return urlunparse((parsed.scheme, parsed.netloc, encoded_path,
                           parsed.params, parsed.query, parsed.fragment))

    # -------------------- SLIKK (ScraperAPI, safer & slower) --------------------

    def extract_slikk_price(self, soup: BeautifulSoup, html_content: str) -> Optional[str]:
        """
        Extract price from Slikk (via ScraperAPI with JS rendering).
        PRIORITY:
          1) "sp"   (selling price)
          2) "offerPrice"/"sellingPrice"/"price"/"mrp"
          3) rupee signs
          4) generic regex
        """
        import json

        sp_match = re.search(r'"sp"\s*:\s*"?(?P<price>\d+\.?\d*)"?', html_content, re.IGNORECASE)
        if sp_match:
            p = self.clean_price(sp_match.group('price'))
            if p:
                return p

        patterns = [
            r'"offerPrice"\s*:\s*"?(?P<price>\d+\.?\d*)"?',
            r'"sellingPrice"\s*:\s*"?(?P<price>\d+\.?\d*)"?',
            r'"price"\s*:\s*"?(?P<price>\d+\.?\d*)"?',
            r'"mrp"\s*:\s*"?(?P<price>\d+\.?\d*)"?',
        ]
        for pat in patterns:
            m = re.search(pat, html_content, re.IGNORECASE)
            if m:
                p = self.clean_price(m.group('price'))
                if p:
                    return p

        next_data_script = soup.find('script', {'id': '__NEXT_DATA__'})
        if next_data_script and next_data_script.string:
            try:
                data = json.loads(next_data_script.string)
                candidates = []

                def deep_search(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            key = k.lower()
                            if key in ('sp', 'offerprice', 'sellingprice', 'price', 'mrp'):
                                candidates.append(str(v))
                            deep_search(v)
                    elif isinstance(obj, list):
                        for item in obj:
                            deep_search(item)

                deep_search(data)

                for c in candidates:
                    p = self.clean_price(c)
                    if p:
                        return p
            except Exception:
                pass

        for tag in soup.find_all(['span', 'div']):
            text = tag.get_text(strip=True)
            if '₹' in text:
                m = re.search(r'₹\s*([\d,]+(?:\.\d{1,2})?)', text)
                if m:
                    p = self.clean_price(m.group(1))
                    if p:
                        return p

        return self.extract_price_generic_regex(html_content)

    def extract_brand_price(self, soup: BeautifulSoup, domain: str, html_content: str) -> Optional[str]:
        """Extract price from brand website with site-specific selectors."""
        price = None

        # ---------------- TIGC ----------------
        if 'tigc.in' in domain:
            element = soup.find('span', attrs={'data-product-price': True})
            if element:
                text = element.get_text()
                if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                    price = text

            if not price:
                selectors = [
                    {'class': 'product__price on-sale'},
                    {'class': 'product__price'},
                    {'class': 'money'},
                    {'class': 'price-item--sale'},
                    {'class': 'price-item--regular'},
                    {'class': 'product-price'},
                    {'class': 'price'}
                ]
                for selector in selectors:
                    element = soup.find('span', selector) or soup.find('div', selector)
                    if element:
                        text = element.get_text()
                        if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                            price = text
                            break

        # ---------------- BEWAKOOF ----------------
        elif 'bewakoof' in domain:
            selectors = [
                {'class': 'productPrice'},
                {'class': 'discountedPriceText'},
                {'class': 'sellingPrice'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if '₹' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- SASSAFRAS ----------------
        elif 'sassafras' in domain:
            element = soup.find('span', attrs={'data-product-price': True})
            if element:
                text = element.get_text()
                if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                    price = text

            if not price:
                selectors = [
                    {'class': 'product__price on-sale'},
                    {'class': 'product__price'},
                    {'class': 'money'},
                    {'class': 'price-item--sale'},
                    {'class': 'price-item--regular'},
                    {'class': 'product-price'},
                    {'class': 'price'}
                ]
                for selector in selectors:
                    element = soup.find('span', selector) or soup.find('div', selector)
                    if element:
                        text = element.get_text()
                        if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                            price = text
                            break

        # ---------------- THE BEAR HOUSE ----------------
        elif 'thebearhouse' in domain or 'bearhouse' in domain:
            element = soup.find('span', attrs={'data-product-price': True})
            if element:
                text = element.get_text()
                if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                    price = text

            if not price:
                selectors = [
                    {'class': 'product__price on-sale'},
                    {'class': 'product__price'},
                    {'class': 'money'},
                    {'class': 'price-item--sale'},
                    {'class': 'price-item--regular'},
                    {'class': 'price'}
                ]
                for selector in selectors:
                    element = soup.find('span', selector) or soup.find('div', selector)
                    if element:
                        text = element.get_text()
                        if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                            price = text
                            break

        # ---------------- MYDESIGNATION ----------------
        elif 'mydesignation' in domain:
            price_element = soup.find('price-money')
            if price_element:
                bdi = price_element.find('bdi')
                if bdi:
                    text = bdi.get_text()
                    price_match = re.search(r'₹\s*([\d,]+)', text)
                    if price_match:
                        price = f"₹{price_match.group(1)}"

            if not price:
                prefix_span = soup.find('span', class_='price__prefix')
                if prefix_span and prefix_span.parent:
                    text = prefix_span.parent.get_text()
                    price_match = re.search(r'₹\s*([\d,]+)', text)
                    if price_match:
                        price = f"₹{price_match.group(1)}"

            if not price:
                selectors = [
                    {'class': 'product-price'},
                    {'class': 'price'},
                    {'class': 'selling-price'},
                    {'class': 'final-price'}
                ]
                for selector in selectors:
                    element = soup.find('span', selector) or soup.find('div', selector)
                    if element:
                        text = element.get_text()
                        if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                            price = text
                            break

        # ---------------- SHOPQISSA / QISSA ----------------
        elif 'shopqissa' in domain or 'qissa' in domain:
            selectors = [
                {'class': 'price price--highlight price--large'},
                {'class': 'price price--highlight'},
                {'class': 'price--highlight'},
                {'class': 'price price--compare'},
                {'class': 'money'},
                {'class': 'price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- CAMPUSSUTRA ----------------
        elif 'campussutra' in domain:
            selectors = [
                {'class': 'price'},
                {'class': 'money'},
                {'class': 'product-price'},
                {'class': 'price-item--sale'},
                {'class': 'price-item--regular'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if 'regular price' in text.lower() or 'sale price' in text.lower():
                        continue
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- BEEGLEE ----------------
        elif 'beeglee' in domain:
            selectors = [
                {'class': 'money'},
                {'class': 'sale-price'},
                {'class': 'price'},
                {'class': 'product-price'},
                {'class': 'price-item--sale'},
                {'class': 'price-item--regular'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if 'sale price' in text.lower() and not any(c.isdigit() for c in text):
                        continue
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- COLOR CAPITAL ----------------
        elif 'colorcapital' in domain:
            selectors = [
                {'class': 'price-item price-item--sale price-item-last'},
                {'class': 'price-item--sale'},
                {'class': 'price-item'},
                {'class': 'money'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if ('sale price' in text.lower() or 'regular price' in text.lower()) and not any(c.isdigit() for c in text):
                        continue
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- MAINCHARACTER ----------------
        elif 'maincharacter' in domain:
            selectors = [
                {'class': 'price-item price-item--sale price-item-last custom-price'},
                {'class': 'price-item--sale'},
                {'class': 'custom-price'},
                {'class': 'price-item'},
                {'class': 'money'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if ('sale price' in text.lower() or 'regular price' in text.lower()) and not any(c.isdigit() for c in text):
                        continue
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- THEATER ----------------
        elif 'theater' in domain:
            selectors = [
                {'class': 'price-item price-item--sale price-item--last'},
                {'class': 'price-item--sale'},
                {'class': 'price-item'},
                {'class': 'money'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if ('sale price' in text.lower() or 'regular price' in text.lower()) and not any(c.isdigit() for c in text):
                        continue
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- BLACKBERRYS ----------------
        elif 'blackberrys' in domain:
            selectors = [
                {'class': 'sale-price'},
                {'class': 'h4 text-subdued'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('xsale-price', selector) or soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if 'mrp' in text.lower() or '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- PINACOLADA ----------------
        elif 'pinacolada' in domain or 'buypinacolada' in domain:
            selectors = [
                {'class': 'product-price__price'},
                {'class': 'product-single__save-amount'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]
            for selector in selectors:
                element = soup.find('s', selector) or soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        if 'save' not in text.lower():
                            price = text
                            break

        # ---------------- SILISOUL ----------------
        elif 'silisoul' in domain:
            selectors = [
                {'class': 'product__price on-sale'},
                {'class': 'product__price'},
                {'class': 'money'},
                {'class': 'price-item--sale'},
                {'class': 'price-item--regular'},
                {'class': 'price'}
            ]
            for selector in selectors:
                element = soup.find('span', selector) or soup.find('div', selector)
                if element:
                    text = element.get_text()
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        price = text
                        break

        # ---------------- VEIRDO ----------------
        elif 'veirdo' in domain:
            element = soup.select_one('span[data-testid="product-price-value"]')
            if element:
                price = element.get_text()

        # ---------------- TECHNOSPORT ----------------
        elif 'technosport' in domain:
            element = soup.select_one('span.m-price-item--sale')
            if element:
                price = element.get_text()

        # ---------------- THE SOULED STORE ----------------
        elif 'thesouledstore' in domain:
            # Try specific selectors for Souled Store
            selectors = [
                'span.offer',
                'span.leftPrice .offer',
                '.offerPrice',
                '.price.offer'
            ]
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    price = element.get_text()
                    if price: break

        # ---------------- JOCKEY ----------------
        elif 'jockey' in domain:
            element = soup.select_one('span.price-item--sale')
            if element:
                price = element.get_text()

        # ---------------- PUMA ----------------
        elif 'puma' in domain:
            element = soup.select_one('span[data-test-id="item-price-pdp"]')
            if element:
                price = element.get_text()

        # If site-specific didn't work, try generic selectors
        if not price:
            selectors = [
                {'attrs': {'data-product-price': True}},
                {'class': 'product__price on-sale'},
                {'class': 'product__price'},
                {'class': 'money'},
                {'class': 'price-item--sale'},
                {'class': 'price-item--regular'},
                {'class': 'price'},
                {'class': 'product-price'}
            ]

            for selector in selectors:
                if 'attrs' in selector:
                    element = soup.find('span', attrs=selector['attrs'])
                else:
                    element = soup.find('span', selector) or soup.find('div', selector)

                if element:
                    text = element.get_text()
                    if '₹' in text or 'Rs' in text or any(c.isdigit() for c in text):
                        if 'sale price' in text.lower() and not any(c.isdigit() for c in text):
                            continue
                        price = self.clean_price(text)
                        if price:
                            return price

        if price:
            return self.clean_price(price)

        return self.extract_price_generic_regex(html_content)

    # -------------------- CLEAN PRICE --------------------

    def clean_price(self, price: str) -> Optional[str]:
        """
        Clean and normalize price string to numeric format.
        """
        if not price:
            return None

        s = str(price)

        # 1. Extract first numeric-like chunk (e.g. 'Rs. 790.00' -> '790.00')
        m = re.search(r'(\d[\d.,]*)', s)
        if not m:
            return None

        cleaned = m.group(1)

        # 2. Remove thousand separators
        cleaned = cleaned.replace(',', '')

        # 3. Remove leading non-digit characters (defensive)
        while cleaned and not cleaned[0].isdigit():
            cleaned = cleaned[1:]

        if not cleaned:
            return None

        # 4. If multiple dots (e.g. '790.00.00'), keep only the first decimal point
        if cleaned.count('.') > 1:
            first, rest = cleaned.split('.', 1)
            rest = rest.replace('.', '')
            cleaned = first + '.' + rest

        try:
            value = float(cleaned)
        except ValueError:
            return None

        if value < 50:
            return None

        return str(int(round(value)))

    # -------------------- FETCH HELPERS --------------------

    def fetch_url(self, url: str, use_scraperapi: bool = False) -> Optional[str]:
        """
        Fetch HTML content from a URL.

        For Myntra/Brand: can retry up to self.max_retries.
        Slikk uses fetch_slikk() instead.
        """
        if not url or url == 'Not Found':
            return None

        from urllib.parse import quote, urlparse, urlunparse

        if ' ' in url:
            parsed = urlparse(url)
            encoded_path = quote(parsed.path, safe='/')
            url = urlunparse((parsed.scheme, parsed.netloc, encoded_path,
                              parsed.params, parsed.query, parsed.fragment))

        for attempt in range(self.max_retries):
            try:
                if use_scraperapi:
                    payload = {
                        'api_key': SCRAPERAPI_KEY,
                        'url': url,
                        'render': 'true'
                    }
                    response = self.session.get(
                        'https://api.scraperapi.com/',
                        params=payload,
                        timeout=60
                    )
                else:
                    headers = {'User-Agent': self._get_random_user_agent()}
                    response = self.session.get(
                        url,
                        headers=headers,
                        timeout=self.timeout
                    )

                response.raise_for_status()
                return response.text

            except requests.exceptions.RequestException:
                if attempt >= self.max_retries - 1:
                    return None
                # retry loop continues

        return None

    def fetch_slikk(self, url: str) -> Optional[str]:
        """
        Fetch HTML for Slikk using ScraperAPI with JS rendering.

        Slower but more reliable:
          - Only a few Slikk calls run at the same time (global semaphore)
          - Retries with small backoff delay if something goes wrong or HTML is tiny
        """
        if not url or url == 'Not Found':
            return None

        # Normalize Slikk URL: product/<id> or encoded path
        url = self.normalize_slikk_url(url)

        last_error = None

        for attempt in range(SLIKK_RETRIES):
            try:
                # Limit how many Slikk calls run at once across all threads
                with slikk_semaphore:
                    payload = {
                        'api_key': SCRAPERAPI_KEY,
                        'url': url,
                        'render': 'true'
                    }
                    response = self.session.get(
                        'https://api.scraperapi.com/',
                        params=payload,
                        timeout=60
                    )

                response.raise_for_status()

                text = response.text or ""
                # If HTML is too small, assume page didn't fully render
                if len(text) < 5000:
                    last_error = ValueError("Slikk HTML too small / incomplete")
                else:
                    return text

            except Exception as e:
                last_error = e

            # If we are here, something went wrong; maybe retry
            if attempt < SLIKK_RETRIES - 1:
                delay = SLIKK_RETRY_DELAY * (attempt + 1)  # 2s, 4s, 6s...
                time.sleep(delay)

        # All retries failed
        return None

    # -------------------- MAIN SCRAPE ROUTER --------------------

    def scrape_price(self, url: str, site_type: str) -> Optional[str]:
        """
        Scrape price from URL based on site type.

        Args:
            url: URL to scrape
            site_type: 'myntra', 'slikk', or 'brand'
        """
        if not url or url == 'Not Found':
            return None

        if site_type == 'slikk':
            html_content = self.fetch_slikk(url)
            if not html_content:
                return None
            soup = BeautifulSoup(html_content, 'html.parser')
            return self.extract_slikk_price(soup, html_content)

        # For The Souled Store, always use ScraperAPI as it requires JS/Anti-bot bypass
        use_api = False
        domain = self.get_domain(url)
        if 'thesouledstore' in domain:
            use_api = True

        html_content = self.fetch_url(url, use_scraperapi=use_api)
        if not html_content:
            return None

        soup = BeautifulSoup(html_content, 'html.parser')

        if site_type == 'myntra':
            return self.extract_myntra_price(soup, html_content)
        elif site_type == 'brand':
            return self.extract_brand_price(soup, domain, html_content)

        return None

# -----------------------------
# PER-ROW PROCESSING (for threads)
# -----------------------------

def process_single_row(
    idx: int,
    row: Dict[str, str],
    extractor: PriceExtractor,
    placeholder_values: set
) -> Tuple[Dict[str, int], int]:
    """
    Process a single product row (Myntra + Slikk + Brand) in a worker thread.

    Returns:
        stats_dict: per-row stats (to be aggregated)
        slikk_api_calls: number of Slikk ScraperAPI hits for this row
    """

    stats = {
        'myntra_success': 0,
        'slikk_success': 0,
        'brand_success': 0,
        'myntra_failed': 0,
        'slikk_failed': 0,
        'slikk_skipped_existing': 0,
        'brand_skipped': 0,
    }
    slikk_api_calls = 0

    # ---------- Myntra ----------
    myntra_url = row.get('myntra_url', '').strip()
    if myntra_url and myntra_url != 'Not Found':
        existing_myntra = row.get('myntra_price', '')
        price = extractor.scrape_price(myntra_url, 'myntra')
        if price:
            row['myntra_price'] = price  # update only on success
            stats['myntra_success'] = 1
        else:
            # keep existing value on failure
            stats['myntra_failed'] = 1

    # ---------- Slikk (ScraperAPI, only when slikk_price not available / placeholder) ----------
    slikk_url = row.get('slikk_url', '').strip()
    if slikk_url and slikk_url != 'Not Found':
        existing_slikk = row.get('slikk_price', '').strip()
        existing_lower = existing_slikk.lower()

        should_skip_existing = (
            not FORCE_UPDATE_SLIKK and
            existing_slikk != '' and
            existing_lower not in placeholder_values
        )

        if should_skip_existing:
            # price is already there -> DO NOT hit ScraperAPI
            stats['slikk_skipped_existing'] = 1
        else:
            # price missing or placeholder -> allowed to hit ScraperAPI
            slikk_api_calls += 1
            price = extractor.scrape_price(slikk_url, 'slikk')
            if price:
                row['slikk_price'] = price  # update only on success
                stats['slikk_success'] = 1
            else:
                stats['slikk_failed'] = 1

    # ---------- Brand ----------
    brand_url = row.get('brand_url', '').strip()
    if brand_url and brand_url != 'Not Found':
        existing_brand = row.get('brand_price', '')
        if extractor.is_brand_supported(brand_url):
            price = extractor.scrape_price(brand_url, 'brand')
            if price:
                row['brand_price'] = price  # update only on success
                stats['brand_success'] = 1
            else:
                # keep existing brand price on failure
                pass
        else:
            stats['brand_skipped'] = 1

    return stats, slikk_api_calls

# -----------------------------
# CSV PROCESSOR
# -----------------------------

def process_csv(input_file: str, output_file: str):
    """
    Process input CSV and scrape prices with high-performance concurrency.
    """
    print("=" * 80)
    print("🛍️  UNIVERSAL PRICE SCRAPER (High Performance + Stable Slikk)")
    print("=" * 80)
    print(f"📥 Input:  {input_file}")
    print(f"📤 Output: {output_file}")
    mode_text = (
        "SMART UPDATE - Slikk only for missing/placeholder prices"
        if not FORCE_UPDATE_SLIKK
        else "FORCE UPDATE - Slikk for all rows"
    )
    print(f"🔄 Mode : {mode_text}")
    print(f"⚙️  Max workers: {MAX_WORKERS}")
    print(f"⚙️  Slikk parallel limit: {SLIKK_MAX_PARALLEL}, retries: {SLIKK_RETRIES}")
    print()

    extractor = PriceExtractor(max_retries=3, timeout=30)

    # Read input CSV
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found!")
        return
    except Exception as e:
        print(f"❌ Error reading CSV: {str(e)}")
        return

    total_rows = len(rows)
    print(f"📊 Total products: {total_rows}")
    print()

    # Ensure all expected fields exist
    for row in rows:
        for field in EXPECTED_FIELDNAMES:
            if field not in row or row[field] is None:
                row[field] = ""

    stats = {
        'total': total_rows,
        'myntra_success': 0,
        'slikk_success': 0,
        'brand_success': 0,
        'myntra_failed': 0,
        'slikk_failed': 0,
        'slikk_skipped_existing': 0,
        'brand_skipped': 0
    }

    placeholder_values = {
        '',
        'not found',
        'product not available on site',
        'price not displayed in listing'
    }

    slikk_api_calls_total = 0

    print("🚀 Starting concurrent scraping...\n")

    processed_count = 0
    PROGRESS_EVERY = max(10, total_rows // 20) if total_rows > 0 else 10

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for idx, row in enumerate(rows, 1):
            futures.append(
                executor.submit(
                    process_single_row,
                    idx,
                    row,
                    extractor,
                    placeholder_values
                )
            )

        for future in as_completed(futures):
            row_stats, slikk_calls = future.result()
            slikk_api_calls_total += slikk_calls

            for k, v in row_stats.items():
                if k in stats:
                    stats[k] += v

            processed_count += 1
            if processed_count % PROGRESS_EVERY == 0 or processed_count == total_rows:
                print(f"   ✅ Processed {processed_count}/{total_rows} rows...")

    # Write output CSV
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=EXPECTED_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        print("\n" + "=" * 80)
        print("✅ COMPLETE! Updated CSV saved.")
        print("=" * 80)

        print(f"\n📊 SUMMARY:")
        print(f"   Total products processed: {stats['total']}")
        print()
        print(f"   🟦 Myntra:")
        print(f"      ✅ Success: {stats['myntra_success']}")
        print(f"      ❌ Failed (kept old values): {stats['myntra_failed']}")
        print()
        print(f"   🟨 Slikk (ScraperAPI, normalized, slower & more stable):")
        print(f"      ✅ Success: {stats['slikk_success']}")
        print(f"      ❌ Failed (kept old values): {stats['slikk_failed']}")
        print(f"      ⏭️  Skipped (existing price): {stats['slikk_skipped_existing']}")
        print(f"      🔍 Debug HTTP calls this run: {slikk_api_calls_total}")
        print()
        print(f"   🟩 Brand URLs:")
        print(f"      ✅ Success: {stats['brand_success']}")
        print(f"      ⏭️  Skipped (unsupported domain): {stats['brand_skipped']}")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error writing output CSV: {str(e)}")


def main():
    """Main function using fixed wow.csv -> pr.csv."""
    print("\n" + "=" * 80)
    print("🛍️  UNIVERSAL PRICE EXTRACTOR (High Performance + Stable Slikk)")
    print("=" * 80)
    print("Supports: Myntra, Slikk (ScraperAPI, selling price 'sp'), and Brand URLs")
    print()
    print(f"📥 Input CSV : {INPUT_CSV}")
    print(f"📤 Output CSV: {OUTPUT_CSV}")
    print("=" * 80)
    print()

    if not Path(INPUT_CSV).exists():
        print(f"❌ Error: Input file '{INPUT_CSV}' not found!")
        sys.exit(1)

    process_csv(INPUT_CSV, OUTPUT_CSV)


if __name__ == "__main__":
    main()
