## rs-web-parser 

compiled rust library (exposed to python via pyo3) for scraping fashion ecommerce product pages.

returns structured data: product name, brand, price, images, garment type, availability.

```json
{
  "product_name": "...",
  "brand": "...",
  "price": { "amount": 250, "currency": "USD" },
  "image_urls": ["..."],
  "garment_type": "upper|lower|full_body|shoes|other|unsupported",
  "availability": "in_stock|out_of_stock|limited|unknown"
}
```

### low-level http/tls

- `wreq` + `wreq-util` + `boringssl` for chrome-grade tls + http/2 emulation (same cipher suites, alpn, etc.).
- `curl-impersonate` binary in the container for hard sites (e.g. therealreal): when `wreq` gets blocked, we shell out to a prebuilt `curl_chrome131_android` that matches what `curl_cffi` did in the python service.
- explicit redirect handling for share/short links (farfetch, ebay, etc.) with a small redirect cap and correct `Location` resolution.

### architecture

- async runtime: tokio.
- http: `wreq`, `wreq-util`, and `curl-impersonate` as a fallback.
- html parsing: `scraper` + a port of `ProductDataExtractor`.
- orchestration: `ScrapeState` tracks fields + source attribution, races all approaches, and decides when to stop.
- pyo3 bindings: exposes `scrape_url(url: String, timeout_secs: f64)` to python.

### build & usage

prereqs: rust toolchain, python 3.8+

```bash
pip install maturin
maturin develop --release
```

verify:

```bash
python -c "import rust_scraper; print(rust_scraper.scrape_url('https://www.zara.com/in/en/leather-effect-bomber-jacket-p04027400.html', timeout_secs=10))"
```

### python api

```python
import rust_scraper

result = rust_scraper.scrape_url(
    "https://www.ssense.com/en-us/women/product/stine-goya/yellow-arum-bikini/10006631",
    timeout_secs=30.0,
)

print(result["product_name"], result["brand"], result["price"])
```

### environment variables

- `GENAI_API_KEY` – google gemini api key
- `SERPAPI_KEY` – serpapi key
- `OXYLABS_PROXY_URL` – proxy url (optional)
- `CLOUDFLARE_WORKER_URL` – headler browser worker endpoint (optional)
