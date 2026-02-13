# Jasoos Engine

Jasoos Engine is a Python-based price intelligence toolkit for fashion products. It discovers matching listings across marketplaces and brand stores, then extracts comparable prices into standardized CSV outputs.

## High-Level Code Architecture

The codebase is split into two pipelines with clear responsibilities:

1. Discovery pipeline (`jaasoos.py`)
- Purpose: Find product URLs and listing-level prices for Myntra, Slikk, and mapped brand websites.
- Core flow:
  - `process_products(...)` orchestrates the end-to-end run.
  - Pass 1 (`process_single_product_pass1`) runs Google Lens search via SerpAPI on product image URLs.
  - Pass 2 (`process_single_product_pass2`) retries only missing sites using site-scoped query strings.
  - `extract_product_info(...)` ranks candidate matches using URL validity checks, relaxed brand matching, and title similarity.
- Important internals:
  - `DualWindowRateLimiter` enforces API throttling (per-second + per-hour).
  - Brand normalization (`get_brand_site`) maps many brand aliases to supported domains.
  - Output is normalized to one row per product with `myntra_*`, `slikk_*`, `brand_*` fields.

2. Price extraction pipeline (`paisa.py`)
- Purpose: Open discovered URLs and extract cleaner/updated prices from page HTML.
- Core flow:
  - `process_csv(...)` reads all rows and runs threaded row workers.
  - `process_single_row(...)` processes Myntra, Slikk, and brand URLs for each row.
  - `PriceExtractor.scrape_price(...)` routes extraction by site type (`myntra`, `slikk`, `brand`).
- Important internals:
  - `PriceExtractor` contains reusable HTTP/session logic and extraction methods.
  - Slikk path uses `fetch_slikk(...)` with ScraperAPI + retries + bounded semaphore for stability.
  - Brand extraction uses domain-specific selector blocks with generic regex fallback.
  - `clean_price(...)` normalizes raw strings into comparable numeric values.

## End-to-End Data Flow

1. Input catalog row (title, brand, image, metadata).
2. `jaasoos.py` discovers external URLs and listing prices.
3. Output CSV from discovery is used as input for `paisa.py`.
4. `paisa.py` fetches pages and updates price fields with extracted values.
5. Final CSV is used for comparison/analysis.

## Project Structure

- `jaasoos.py`: Discovery + matching using SerpAPI/Google Lens.
- `paisa.py`: URL scraping + site-specific price extraction.

## Dependencies

Install required libraries:

```bash
pip install requests beautifulsoup4 urllib3
```

## Configuration

### `jaasoos.py`
- Uses `SERPAPI_KEY` environment variable (there is currently a fallback key in code).
- Includes built-in rate limits:
  - max 7 requests/second
  - max 1000 requests/hour

### `paisa.py`
- Uses `SCRAPERAPI_KEY` (currently hardcoded in the script).
- Concurrency controls:
  - `MAX_WORKERS`
  - `SLIKK_MAX_PARALLEL`
  - `SLIKK_RETRIES` and retry delay

## Input/Output Schemas

### `jaasoos.py`
Default input file in script:
- `bew.csv`

Reads columns:
- `style_id`
- `brand_name`
- `product_title`
- `gender`
- `category`
- `min_price_rupees`
- `first_image_url`
- `view_count`

Default output file in script:
- `bewa.csv`

Writes columns:
- `view_count`
- `style_id`
- `brand`
- `product_title`
- `gender`
- `category`
- `klydo_price`
- `myntra_price`
- `slikk_price`
- `brand_price`
- `klydo_url`
- `myntra_url`
- `slikk_url`
- `brand_url`

### `paisa.py`
Default input/output in script:
- Input: `soulded-store-20jan.csv`
- Output: `soul-store20jan.csv`

Expected columns:
- `view_count`
- `style_id`
- `brand`
- `product_title`
- `gender`
- `category`
- `klydo_price`
- `myntra_price`
- `slikk_price`
- `brand_price`
- `klydo_url`
- `myntra_url`
- `slikk_url`
- `brand_url`

## How to Run

```bash
cd "/Users/arnavshukla/Documents/Development/Jasoos/jasoos 2"
python3 jaasoos.py
python3 paisa.py
```

## Operational Notes

- Filenames are currently hardcoded in both scripts; update constants before each run.
- API keys should be moved to environment variables before production usage.
- Network-heavy jobs can take significant time based on row volume and website response quality.
