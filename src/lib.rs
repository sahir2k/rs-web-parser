mod html_extractor;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;
use scraper::Html;
use url::Url;
use lazy_static::lazy_static;
use regex::Regex;

use crate::html_extractor::ProductDataExtractor;

// ==================== CONFIG ====================

const MOBILE_UA: &str = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36";

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

// ==================== DATA STRUCTURES ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Price {
    amount: Option<i32>,
    currency: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProductData {
    product_name: Option<String>,
    brand: Option<String>,
    price: Option<Price>,
    image_urls: Vec<String>,
    garment_type: Option<String>,
    availability: Option<String>,
}

impl ProductData {
    fn is_complete(&self) -> bool {
        self.product_name.is_some()
            && self.brand.is_some()
            && self.price.as_ref().and_then(|p| p.amount).is_some()
            && !self.image_urls.is_empty()
            && self.garment_type.is_some()
    }

    fn missing_fields(&self) -> Vec<&str> {
        let mut missing = Vec::new();
        if self.product_name.is_none() {
            missing.push("product_name");
        }
        if self.brand.is_none() {
            missing.push("brand");
        }
        if self.price.as_ref().and_then(|p| p.amount).is_none() {
            missing.push("price");
        }
        if self.image_urls.is_empty() {
            missing.push("image_urls");
        }
        if self.garment_type.is_none() {
            missing.push("garment_type");
        }
        missing
    }
}

#[derive(Clone)]
struct ScrapeState {
    product: Arc<Mutex<ProductData>>,
    field_attribution: Arc<Mutex<HashMap<String, String>>>,
    start_time: Instant,
}

impl ScrapeState {
    fn new() -> Self {
        Self {
            product: Arc::new(Mutex::new(ProductData::default())),
            field_attribution: Arc::new(Mutex::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    fn elapsed_ms(&self) -> u128 {
        self.start_time.elapsed().as_millis()
    }

    async fn merge_data(&self, incoming: &HashMap<String, serde_json::Value>, source: &str) {
        let mut product = self.product.lock().await;
        let mut attribution = self.field_attribution.lock().await;
        let mut merged_fields: Vec<&str> = Vec::new();

        fn source_priority(src: &str) -> u8 {
            match src {
                // strong html+gemini sources
                "curlcffi_gemini" | "curlcffi_gemini_proxy" | "requests_gemini" | "cloudflare_gemini" => 0,
                // title-based gemini classification
                "gemini_classification" => 1,
                // serpapi shopping
                "serpapi_google" => 2,
                // fast url classifier
                "gemini_fast" => 3,
                // image-only helpers
                "serpapi_images_url" | "serpapi_images_title" => 4,
                _ => 5,
            }
        }

        fn should_override_field(
            field: &str,
            source: &str,
            attribution: &HashMap<String, String>,
            is_empty: bool,
        ) -> bool {
            if is_empty {
                return true;
            }
            if let Some(existing_src) = attribution.get(field) {
                source_priority(source) < source_priority(existing_src)
            } else {
                false
            }
        }

        // product_name
        if let Some(name) = incoming
            .get("product_name")
            .or_else(|| incoming.get("name"))
            .or_else(|| incoming.get("title"))
            .and_then(|v| v.as_str())
        {
            let is_empty = product.product_name.is_none();
            if should_override_field("product_name", source, &attribution, is_empty) {
                product.product_name = Some(name.to_string());
                attribution.insert("product_name".to_string(), source.to_string());
                merged_fields.push("product_name");
            }
        }

        // brand
        if let Some(brand) = incoming.get("brand").and_then(|v| v.as_str()) {
            let is_empty = product.brand.is_none();
            if should_override_field("brand", source, &attribution, is_empty) {
                product.brand = Some(brand.to_string());
                attribution.insert("brand".to_string(), source.to_string());
                merged_fields.push("brand");
            }
        }

        // price
        if let Some(price_val) = incoming.get("price") {
            let parsed = parse_price(price_val);
            if parsed.amount.is_some() {
                let is_empty = product.price.as_ref().and_then(|p| p.amount).is_none();
                if should_override_field("price", source, &attribution, is_empty) {
                    product.price = Some(parsed);
                    attribution.insert("price".to_string(), source.to_string());
                    merged_fields.push("price");
                }
            }
        }

        // image_urls (support both "image_urls" and "images" keys)
        if let Some(images) = incoming
            .get("image_urls")
            .or_else(|| incoming.get("images"))
            .and_then(|v| v.as_array())
        {
            let urls: Vec<String> = images
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            let is_empty = product.image_urls.is_empty();
            // Prefer sources with more images; on tie, use priority.
            let should_take = urls.len() > product.image_urls.len()
                || (urls.len() == product.image_urls.len()
                    && should_override_field("image_urls", source, &attribution, is_empty));

            if should_take {
                product.image_urls = urls;
                attribution.insert("image_urls".to_string(), source.to_string());
                merged_fields.push("image_urls");
            }
        }

        // garment_type
        if let Some(gtype) = incoming.get("garment_type").and_then(|v| v.as_str()) {
            let is_empty = product.garment_type.is_none();
            if should_override_field("garment_type", source, &attribution, is_empty) {
                product.garment_type = Some(gtype.to_string());
                attribution.insert("garment_type".to_string(), source.to_string());
                merged_fields.push("garment_type");
            }
        }

        // availability
        if let Some(status) = incoming.get("availability").and_then(|v| v.as_str()) {
            let is_empty = product.availability.is_none();
            if should_override_field("availability", source, &attribution, is_empty) {
                product.availability = Some(status.to_string());
                attribution.insert("availability".to_string(), source.to_string());
                merged_fields.push("availability");
            }
        }

        if !merged_fields.is_empty() {
            let elapsed = self.elapsed_ms();
            println!(
                "[rust_scraper] +{}ms merge_data from {}: {:?}",
                elapsed, source, merged_fields
            );
        }
    }

    async fn is_complete(&self) -> bool {
        self.product.lock().await.is_complete()
    }

    async fn has_strong_source(&self) -> bool {
        let attribution = self.field_attribution.lock().await;
        attribution.values().any(|src| {
            matches!(
                src.as_str(),
                "curlcffi_gemini"
                    | "curlcffi_gemini_proxy"
                    | "requests_gemini"
                    | "cloudflare_gemini"
            )
        })
    }
}

// ==================== UTILITY FUNCTIONS ====================

fn parse_price(value: &serde_json::Value) -> Price {
    if let Some(obj) = value.as_object() {
        return Price {
            amount: obj.get("amount").and_then(|v| v.as_i64()).map(|v| v as i32),
            currency: obj.get("currency").and_then(|v| v.as_str()).map(String::from),
        };
    }

    if let Some(num) = value.as_f64() {
        return Price {
            amount: Some(num as i32),
            currency: Some("USD".to_string()),
        };
    }

    if let Some(s) = value.as_str() {
        return parse_price_string(s);
    }

    Price {
        amount: None,
        currency: None,
    }
}

fn parse_price_string(s: &str) -> Price {
    let mut currency = None;
    let mut price_str = s.to_string();

    // Strip "Was" prefix
    if price_str.contains("Was") {
        // Remove the word "Was" anywhere and trim
        price_str = price_str.replace("Was", "");
    }

    // Currency symbols
    if price_str.contains("A$") {
        currency = Some("AUD".to_string());
        price_str = price_str.replace("A$", "");
    } else if price_str.contains("C$") {
        currency = Some("CAD".to_string());
        price_str = price_str.replace("C$", "");
    } else if price_str.contains('$') {
        currency = Some("USD".to_string());
        price_str = price_str.replace('$', "");
    } else if price_str.contains('€') {
        currency = Some("EUR".to_string());
        price_str = price_str.replace('€', "");
    } else if price_str.contains('£') {
        currency = Some("GBP".to_string());
        price_str = price_str.replace('£', "");
    } else if price_str.contains('¥') {
        currency = Some("JPY".to_string());
        price_str = price_str.replace('¥', "");
    } else if price_str.contains('₹') {
        currency = Some("INR".to_string());
        price_str = price_str.replace('₹', "");
    }

    // Extract digits
    price_str = price_str.replace(',', "").trim().to_string();
    let amount = if price_str.contains('.') {
        price_str.parse::<f64>().ok().map(|v| v as i32)
    } else {
        let digits: String = price_str.chars().filter(|c| c.is_ascii_digit()).collect();
        digits.parse::<i32>().ok()
    };

    Price {
        amount,
        currency: currency.or_else(|| Some("USD".to_string())),
    }
}

fn normalize_domain(url: &str) -> Option<String> {
    let host = Url::parse(url).ok()?.host_str()?.to_lowercase();
    if host.starts_with("www.") {
        Some(host[4..].to_string())
    } else {
        Some(host)
    }
}

fn normalize_url_path(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let segments: Vec<_> = parsed
        .path()
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let mut cleaned_segments = Vec::new();
    for seg in segments {
        let lower = seg.to_lowercase();
        if lower.len() == 5
            && (lower.as_bytes()[2] == b'-' || lower.as_bytes()[2] == b'_')
            && lower[..2].chars().all(|c| c.is_ascii_alphabetic())
            && lower[3..].chars().all(|c| c.is_ascii_alphabetic())
        {
            continue;
        }
        cleaned_segments.push(seg);
    }
    let new_path = format!("/{}", cleaned_segments.join("/"));
    let mut rebuilt = parsed;
    rebuilt.set_path(&new_path);
    rebuilt.set_query(None);
    rebuilt.set_fragment(None);
    Some(rebuilt.to_string())
}

fn clean_product_url(url: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        let mut kept: Vec<(String, String)> = Vec::new();
        for (k, v) in parsed.query_pairs() {
            let key = k.to_string();
            let key_lower = key.to_lowercase();
            if ["pid", "productid", "product_id", "id", "item", "itemid", "product_no", "products_id", "main_page"]
                .contains(&key_lower.as_str())
            {
                kept.push((key, v.to_string()));
            }
        }
        parsed.set_query(None);
        if !kept.is_empty() {
            let mut new_q = String::new();
            for (idx, (k, v)) in kept.into_iter().enumerate() {
                if idx > 0 {
                    new_q.push('&');
                }
                new_q.push_str(&urlencoding::encode(&k));
                new_q.push('=');
                new_q.push_str(&urlencoding::encode(&v));
            }
            parsed.set_query(Some(&new_q));
        }
        parsed.to_string()
    } else {
        url.to_string()
    }
}

fn urls_match_product(url1: &str, url2: &str) -> bool {
    let domain1 = match normalize_domain(url1) {
        Some(d) => d,
        None => return false,
    };
    let domain2 = match normalize_domain(url2) {
        Some(d) => d,
        None => return false,
    };
    if domain1 != domain2 {
        return false;
    }
    let norm1 = normalize_url_path(url1).unwrap_or_else(|| url1.to_string());
    let norm2 = normalize_url_path(url2).unwrap_or_else(|| url2.to_string());
    norm1 == norm2
}

fn fetch_with_curl_impersonate(url: &str) -> Option<String> {
    let output = Command::new("/opt/curl_chrome131_android")
        .arg("-sS")
        .arg(url)
        .output()
        .ok()?;

    if !output.status.success() {
        println!(
            "[rust_scraper] curl-impersonate exit_code={} url={}",
            output.status, url
        );
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    if stdout.is_empty() {
        println!("[rust_scraper] curl-impersonate returned empty body url={}", url);
        return None;
    }

    println!(
        "[rust_scraper] curl-impersonate fetched {} bytes url={}",
        stdout.len(),
        url
    );
    Some(stdout)
}

// ==================== HTML EXTRACTION ====================

fn extract_product_data_from_html(url: &str, html: &str) -> serde_json::Value {
    let extractor = ProductDataExtractor::new(50_000);
    extractor.extract_product_data(url, html)
}

// ==================== GEMINI CLIENT ====================

async fn call_gemini_for_product_extraction(
    url_for_log: &str,
    extracted_data: &serde_json::Value,
    client: &wreq::Client,
) -> Option<HashMap<String, serde_json::Value>> {
    let genai_key = env_var("GENAI_API_KEY")?;
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent?key={}",
        genai_key
    );

    let prompt = format!(
        r#"
You are a product data extraction expert. Analyze the provided webpage data to extract clothing information.

YOUR TASK:

1. Determine if this is a product page (is_product_page: true/false)
   - If NOT a product page (homepage, category, blog), return is_product_page: false with other fields empty

2. If it IS a product page, extract:
   - product_name: Full product name/title (concise, no descriptions)
   - brand: Brand or manufacturer name
   - price: Price with currency symbol (e.g., "$1,200", "€850", "₹2,699")
     * PRIORITY: Look in JSON-LD/structured_data first (offers.price, og:price:amount) and fallback to price_signals array and use the below logic.
     * If you see multiple prices (e.g., "Now $25.00+" and "Original Price: $50.00+"), return the LOWER price (the current/sale price)
     * If only a price range exists (e.g., "$25-$50"), return the lower bound
     * Return empty string if no valid price found
   - garment_type: Classify the clothing type. "upper" for tops/outerwear (shirts, jackets, etc.), "lower" for bottoms (pants, shorts, skirts, etc.), "full_body" for anything that would be a full outfit, like dresses, loungewear, pajamas, full body suits, etc. , "shoes" for footwear, "other" for accessories (bags, hats, jewelry), "unsupported" for non-clothing items (e.g. toys, furniture, electronics, etc.)
   - gender: Infer the target gender for this product. Return "male" for menswear, "female" for womenswear. Look for keywords in product name, category, URL, or structured data (e.g., "men's", "women's", "ladies", "mens"). 
   - image_urls: Extract EVERY valid product image URL from the data. CRITICAL INSTRUCTIONS:
     * If "images" array exists: Include EVERY URL from it (all angles, all colors, all variants)
     * Skip URLs containing "data:image/", "favicon", "icon", "logo", or ending with ".gif" - basically whatever doesn't feel like a product image
     * If "images" array is empty/missing: Use "structured_data.open_graph.og:image" as fallback (only if it's a valid http/https URL)
     * NEVER limit the number of images - if there are 10 images, return all 10. If there are 20 images, return all 20
     * Only return empty array [] if absolutely no valid image URLs exist in the entire data structure
   - availability: Stock status. Check og:availability meta tags, JSON-LD availability field, and button/text content ("Add to Cart", "Out of Stock", "Sold Out", "In Stock"). Return one of: "in_stock", "out_of_stock", "limited", "unknown"

FOCUS ON:
- Use structured data (JSON-LD, Open Graph meta tags) as primary source when available
- If structured data is missing or incomplete, use text content to infer missing information
- For gender: Look at product title, category, URL path, and any gender-specific keywords
- For images: Return EVERY image URL from the "images" array - do not filter, do not limit, do not select a subset
---

WEBPAGE DATA:
{}
"#,
        serde_json::to_string_pretty(extracted_data).unwrap()
    );

    let payload = serde_json::json!({
        "contents": [{
            "role": "user",
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "is_product_page": {"type": "boolean"},
                    "product_name": {"type": "string"},
                    "brand": {"type": "string"},
                    "price": {"type": "string"},
                    "garment_type": {
                        "type": "string",
                        "enum": ["upper", "lower", "full_body", "shoes", "other", "unsupported"]
                    },
                    "gender": {
                        "type": "string",
                        "enum": ["male", "female"]
                    },
                    "image_urls": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "availability": {
                        "type": "string",
                        "enum": ["in_stock", "out_of_stock", "limited", "unknown"]
                    }
                },
                "required": ["is_product_page"]
            }
        }
    });

    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await
        .ok()?;

    let result: serde_json::Value = resp.json().await.ok()?;

    let raw_text = result
        .get("candidates")?
        .get(0)?
        .get("content")?
        .get("parts")?
        .get(0)?
        .get("text")?
        .as_str()?;

    let mut text = raw_text.trim().to_string();

    if text.starts_with("```") {
        let mut t = text.clone();
        if t.starts_with("```json") {
            t = t.chars().skip(7).collect();
        } else {
            t = t.chars().skip(3).collect();
        }
        if t.ends_with("```") {
            t.truncate(t.len().saturating_sub(3));
        }
        text = t.trim().to_string();
    }

    let parsed: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            println!("[rust_scraper] [gemini] JSON parse error: {e}, attempting to fix...");
            println!("[rust_scraper] [gemini] Problematic JSON: {}", text);
            let re = Regex::new(r",(\s*[}\]])").unwrap();
            let fixed = re.replace_all(&text, "$1").to_string();
            match serde_json::from_str(&fixed) {
                Ok(v) => v,
                Err(_) => {
                    println!("[rust_scraper] [gemini] Could not fix JSON after attempted repair");
                    return None;
                }
            }
        }
    };

    if let Some(is_product_page) = parsed.get("is_product_page").and_then(|v| v.as_bool()) {
        if !is_product_page {
            let snippet = serde_json::to_string(&parsed).unwrap_or_default();
            let snippet = if snippet.len() > 500 {
                &snippet[..500]
            } else {
                &snippet
            };
            println!(
                "[rust_scraper] [gemini] is_product_page=false url={} response_snippet={}",
                url_for_log, snippet
            );
            return None;
        }
    } else {
        println!(
            "[rust_scraper] [gemini] missing is_product_page url={}",
            url_for_log
        );
        return None;
    }

    let mut extracted = HashMap::new();
    if let Some(name) = parsed.get("product_name").and_then(|v| v.as_str()) {
        extracted.insert("product_name".to_string(), serde_json::Value::String(name.to_string()));
    }
    if let Some(brand) = parsed.get("brand").and_then(|v| v.as_str()) {
        extracted.insert("brand".to_string(), serde_json::Value::String(brand.to_string()));
    }
    if let Some(price) = parsed.get("price") {
        // Preserve Gemini's raw price value (string, number, or object).
        // parse_price() will normalize this into Price { amount, currency }.
        extracted.insert("price".to_string(), price.clone());
    }
    if let Some(gtype) = parsed.get("garment_type").and_then(|v| v.as_str()) {
        extracted.insert("garment_type".to_string(), serde_json::Value::String(gtype.to_string()));
    }
    if let Some(images) = parsed.get("image_urls").and_then(|v| v.as_array()) {
        extracted.insert("image_urls".to_string(), serde_json::Value::Array(images.clone()));
    }

    Some(extracted)
}

// ==================== FAST GEMINI URL CLASSIFIER ====================

async fn call_gemini_for_fast_classification(
    url: &str,
    client: &wreq::Client,
) -> Option<HashMap<String, serde_json::Value>> {
    let genai_key = env_var("GENAI_API_KEY")?;

    // Strip query parameters and fragment for cleaner classification
    let cleaned_url = Url::parse(url).ok().map(|parsed| {
        Url::parse(&format!(
            "{}://{}{}",
            parsed.scheme(),
            parsed.host_str().unwrap_or_default(),
            parsed.path()
        ))
        .ok()
        .map(|u| u.to_string())
        .unwrap_or_else(|| url.to_string())
    }).unwrap_or_else(|| url.to_string());

    let genai_url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent?key={}",
        genai_key
    );

    let prompt = format!(
        r#"
Analyze the URL below to determine if it's a SINGLE PRODUCT PAGE or a CATEGORY/LISTING PAGE.

CRITICAL RULES - Check these FIRST:

🚫 IMMEDIATELY return "unsupported" if the URL contains ONLY:
   - Plural category words: sweaters, jackets, dresses, pants, shoes, boots, cardigans, jumpers, hoodies, etc.
   - Generic navigation: men, women, clothing, accessories, collections, shop, brands, designers
   - No specific product identifier at the end

Examples that MUST return "unsupported":
  ❌ /sweaters/sweaters-and-cardigans (plural categories only)
  ❌ /men/pants (gender + category only)
  ❌ /designers/golden-goose (browsing a designer)
  ❌ /brands/nike (browsing a brand)
  ❌ /collections/summer (browsing a collection)
  ❌ /shop/outerwear (browsing a category)


✅ Examples that ARE product pages (have specific identifiers):
  ✓ /products/blue-denim-jacket-abc123 (has unique product name + ID)
  ✓ /men/sneakers/air-max-97-white (has specific model name)
  ✓ /cashmere-crewneck-sweater-navy (specific product with descriptors)
  ✓ /p/abc123 (has product ID)

If this is a CATEGORY/LISTING page, return "unsupported" now and STOP.

---

If this IS a single product page, classify it as ONE of these garment types:
- "upper": tops, shirts, jackets, hoodies, sweaters, cardigans, vests, coats, etc.
- "lower": pants, shorts, skirts, leggings, trousers, etc.
- "full_body": dresses, jumpsuits, rompers, loungewear sets, pajama sets, full suits
- "shoes": all footwear (sneakers, boots, sandals, etc.)
- "other": fashion accessories (bags, hats, jewelry, belts, watches, scarves, sunglasses)
- "unsupported": NOT a fashion item (furniture, electronics, toys, etc.)

Return ONLY the garment_type as a single word inside JSON.

---

URL: {}
"#,
        cleaned_url
    );

    let payload = serde_json::json!({
        "contents": [{
            "role": "user",
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "garment_type": {
                        "type": "string",
                        "enum": ["upper", "lower", "full_body", "shoes", "other", "unsupported"]
                    }
                },
                "required": ["garment_type"]
            }
        }
    });

    let resp = client.post(&genai_url)
        .json(&payload)
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let result: serde_json::Value = resp.json().await.ok()?;
    let mut text = result
        .get("candidates")?
        .get(0)?
        .get("content")?
        .get("parts")?
        .get(0)?
        .get("text")?
        .as_str()?
        .trim()
        .to_string();

    if text.starts_with("```") {
        let mut t = text.clone();
        if t.starts_with("```json") {
            t = t.chars().skip(7).collect();
        } else {
            t = t.chars().skip(3).collect();
        }
        if t.ends_with("```") {
            t.truncate(t.len().saturating_sub(3));
        }
        text = t.trim().to_string();
    }

    let parsed: serde_json::Value = serde_json::from_str(&text).ok()?;
    let gtype = parsed.get("garment_type").and_then(|v| v.as_str()).unwrap_or("unsupported");

    let mut out = HashMap::new();
    out.insert(
        "garment_type".to_string(),
        serde_json::Value::String(gtype.to_string()),
    );
    Some(out)
}

// ==================== SERPAPI CLIENT ====================

async fn serpapi_search(
    params: &HashMap<String, String>,
    client: &wreq::Client,
) -> Option<serde_json::Value> {
    let mut url = Url::parse("https://serpapi.com/search").ok()?;
    for (k, v) in params {
        url.query_pairs_mut().append_pair(k, v);
    }

    let resp = client.get(url.as_str()).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.json().await.ok()
}

// ==================== GEMINI CLASSIFICATION FROM SERPAPI ====================

async fn call_gemini_from_serpapi(
    url: &str,
    title: &str,
    snippet: Option<&str>,
    client: &wreq::Client,
) -> Option<HashMap<String, serde_json::Value>> {
    if title.is_empty() {
        return None;
    }

    let genai_key = env_var("GENAI_API_KEY")?;
    let genai_url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={}",
        genai_key
    );

    let description_line = snippet
        .filter(|s| !s.is_empty())
        .map(|s| format!("\nDescription: {}", s))
        .unwrap_or_default();

    let prompt = format!(
        "Title: {}{}\
\nURL: {}\n\
Analyze this product and determine:\n\
1. The garment type\n\
2. The product name (without the brand name)\n\
3. The brand name\n\n\
For garment type, by upper item we mean e.g. shirt, blouse, sweater, jacket, outerwear, blazer, cardigan, vest, tank top etc. \
By lower item we mean e.g. pants, shorts, jeans, skirt, leggings, trousers etc. \
By full_body we mean e.g. dress, jumpsuit, long coat, romper, overalls etc. \
By shoes we mean any footwear like sneakers, boots, sandals, heels, loafers etc. \
If it's a fashion accessory like a bag, hat, scarf, belt, jewelry, sunglasses etc, then it's other. \
Only use 'unsupported' if this is clearly not a fashion/clothing/accessory product at all (e.g. electronics, furniture, kitchenware).\n\n\
Return as JSON with fields 'brand', 'name', and 'garment_type'.",
        title,
        description_line,
        url
    );

    let payload = serde_json::json!({
        "contents": [{
            "role": "user",
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "temperature": 0.0,
            "topK": 1,
            "topP": 0.1,
            "maxOutputTokens": 200,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "brand": {"type": "string"},
                    "name": {"type": "string"},
                    "garment_type": {
                        "type": "string",
                        "enum": ["upper", "lower", "full_body", "shoes", "other", "unsupported"]
                    }
                },
                "required": ["brand", "name", "garment_type"]
            }
        }
    });

    let resp = client.post(&genai_url)
        .json(&payload)
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let result: serde_json::Value = resp.json().await.ok()?;
    let text = result
        .get("candidates")?
        .get(0)?
        .get("content")?
        .get("parts")?
        .get(0)?
        .get("text")?
        .as_str()?;

    let parsed: serde_json::Value = serde_json::from_str(text).ok()?;
    let mut out = HashMap::new();

    if let Some(name) = parsed.get("name").and_then(|v| v.as_str()) {
        out.insert("name".to_string(), serde_json::Value::String(name.to_string()));
    }
    if let Some(brand) = parsed.get("brand").and_then(|v| v.as_str()) {
        out.insert("brand".to_string(), serde_json::Value::String(brand.to_string()));
    }
    if let Some(gtype) = parsed.get("garment_type").and_then(|v| v.as_str()) {
        out.insert("garment_type".to_string(), serde_json::Value::String(gtype.to_string()));
    }

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

// ==================== FETCH FUNCTIONS ====================

async fn fetch_html_curlcffi(original_url: &str, _client: &wreq::Client) -> Option<String> {
    // Create Chrome-impersonating client with wreq
    let chrome_client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Chrome131)
        .build()
        .ok()?;

    let mut current_url = original_url.to_string();
    let max_redirects = 3;

    for _ in 0..=max_redirects {
        // First attempt with default emulation
        let mut resp = chrome_client.get(&current_url).send().await.ok()?;
        let mut status = resp.status();

        // If forbidden, retry with mobile User-Agent
        if status.as_u16() == 403 {
            println!(
                "[rust_scraper] curlcffi_gemini HTTP 403, retrying with mobile UA url={}",
                current_url
            );
            resp = chrome_client
                .get(&current_url)
                .header("User-Agent", MOBILE_UA)
                .send()
                .await
                .ok()?;
            status = resp.status();
        }

        let code = status.as_u16();

        // Successful response: return body
        if status.is_success() {
            let text = resp.text().await.ok()?;
            println!(
                "[rust_scraper] curlcffi_gemini fetched {} bytes status={} url={}",
                text.len(),
                status,
                current_url
            );
            return Some(text);
        }

        // Handle HTTP redirects (3xx) by following Location header, similar to Python curl_cffi.
        if (300..400).contains(&code) {
            if let Some(loc_hdr) = resp.headers().get("location") {
                if let Ok(loc_str) = loc_hdr.to_str() {
                    // Resolve relative redirects against the current URL
                    let next_url = if let Ok(base) = Url::parse(&current_url) {
                        match base.join(loc_str) {
                            Ok(u) => u.to_string(),
                            Err(_) => loc_str.to_string(),
                        }
                    } else {
                        loc_str.to_string()
                    };
                    println!(
                        "[rust_scraper] curlcffi_gemini redirect {} -> {}",
                        current_url, next_url
                    );
                    current_url = next_url;
                    continue;
                }
            }
            println!(
                "[rust_scraper] curlcffi_gemini HTTP {} with no usable Location header url={}",
                code, current_url
            );
            return None;
        }

        // Non-success, non-redirect: for some hard domains (e.g., therealreal.com),
        // fall back to curl-impersonate.
        if let Ok(parsed) = Url::parse(&current_url) {
            if let Some(host) = parsed.host_str() {
                if host.contains("therealreal.com") {
                    if let Some(body) = fetch_with_curl_impersonate(&current_url) {
                        return Some(body);
                    }
                }
            }
        }
        println!(
            "[rust_scraper] curlcffi_gemini HTTP status={} url={}",
            status, current_url
        );
        return None;
    }

    println!(
        "[rust_scraper] curlcffi_gemini exceeded redirect limit starting from url={}",
        original_url
    );
    None
}

async fn fetch_html_curlcffi_proxy(original_url: &str) -> Option<String> {
    let proxy_url = env_var("OXYLABS_PROXY_URL")?;
    let proxy = wreq::Proxy::all(&proxy_url).ok()?;

    let proxy_client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Chrome131)
        .proxy(proxy)
        .build()
        .ok()?;

    let mut current_url = original_url.to_string();
    let max_redirects = 3;

    for _ in 0..=max_redirects {
        // First attempt with default emulation
        let mut resp = proxy_client.get(&current_url).send().await.ok()?;
        let mut status = resp.status();

        // If forbidden, retry with mobile User-Agent
        if status.as_u16() == 403 {
            println!(
                "[rust_scraper] curlcffi_gemini_proxy HTTP 403, retrying with mobile UA url={}",
                current_url
            );
            resp = proxy_client
                .get(&current_url)
                .header("User-Agent", MOBILE_UA)
                .send()
                .await
                .ok()?;
            status = resp.status();
        }

        let code = status.as_u16();

        // Successful response: return body
        if status.is_success() {
            let text = resp.text().await.ok()?;
            println!(
                "[rust_scraper] curlcffi_gemini_proxy fetched {} bytes status={} url={}",
                text.len(),
                status,
                current_url
            );
            return Some(text);
        }

        // Handle HTTP redirects (3xx) by following Location header
        if (300..400).contains(&code) {
            if let Some(loc_hdr) = resp.headers().get("location") {
                if let Ok(loc_str) = loc_hdr.to_str() {
                    let next_url = if let Ok(base) = Url::parse(&current_url) {
                        match base.join(loc_str) {
                            Ok(u) => u.to_string(),
                            Err(_) => loc_str.to_string(),
                        }
                    } else {
                        loc_str.to_string()
                    };
                    println!(
                        "[rust_scraper] curlcffi_gemini_proxy redirect {} -> {}",
                        current_url, next_url
                    );
                    current_url = next_url;
                    continue;
                }
            }
            println!(
                "[rust_scraper] curlcffi_gemini_proxy HTTP {} with no usable Location header url={}",
                code, current_url
            );
            return None;
        }

        // Non-success, non-redirect: allow curl-impersonate fallback for specific domains
        if let Ok(parsed) = Url::parse(&current_url) {
            if let Some(host) = parsed.host_str() {
                if host.contains("therealreal.com") {
                    if let Some(body) = fetch_with_curl_impersonate(&current_url) {
                        return Some(body);
                    }
                }
            }
        }
        println!(
            "[rust_scraper] curlcffi_gemini_proxy HTTP status={} url={}",
            status, current_url
        );
        return None;
    }

    println!(
        "[rust_scraper] curlcffi_gemini_proxy exceeded redirect limit starting from url={}",
        original_url
    );
    None
}

async fn fetch_cloudflare_worker_data(url: &str, client: &wreq::Client) -> Option<serde_json::Value> {
    let encoded_url = urlencoding::encode(url);
    let worker_url = env_var("CLOUDFLARE_WORKER_URL")?;
    let final_url = format!("{}?url={}", worker_url, encoded_url);

    let resp = client.get(&final_url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let json: serde_json::Value = resp.json().await.ok()?;
    if json.get("error").is_some() {
        return None;
    }

    Some(json)
}

// ==================== APPROACH IMPLEMENTATIONS ====================

async fn approach_curlcffi_gemini(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let html = fetch_html_curlcffi(url, client).await?;
    let extracted = extract_product_data_from_html(url, &html);
    let gemini_result = call_gemini_for_product_extraction(url, &extracted, client).await?;

    state.merge_data(&gemini_result, "curlcffi_gemini").await;
    Some(())
}

async fn approach_curlcffi_gemini_proxy(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let html = fetch_html_curlcffi_proxy(url).await?;
    let extracted = extract_product_data_from_html(url, &html);
    let gemini_result = call_gemini_for_product_extraction(url, &extracted, client).await?;

    state.merge_data(&gemini_result, "curlcffi_gemini_proxy").await;
    Some(())
}

async fn approach_requests_gemini(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let html = resp.text().await.ok()?;

    let extracted = extract_product_data_from_html(url, &html);
    let gemini_result = call_gemini_for_product_extraction(url, &extracted, client).await?;

    state.merge_data(&gemini_result, "requests_gemini").await;
    Some(())
}

async fn approach_cloudflare_gemini(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let data = fetch_cloudflare_worker_data(url, client).await?;
    let gemini_result = call_gemini_for_product_extraction(url, &data, client).await?;

    state.merge_data(&gemini_result, "cloudflare_gemini").await;
    Some(())
}

async fn approach_serpapi_google(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let cleaned = clean_product_url(url);

    let mut params = HashMap::new();
    params.insert("engine".to_string(), "google_shopping_light".to_string());
    params.insert("q".to_string(), cleaned.clone());
    params.insert("gl".to_string(), "us".to_string());
    params.insert("hl".to_string(), "en".to_string());
    let serp_key = env_var("SERPAPI_KEY")?;
    params.insert("api_key".to_string(), serp_key);
    params.insert("google_domain".to_string(), "google.com".to_string());

    // First attempt
    let mut result = serpapi_search(&params, client).await;

    // If no shopping_results, retry with normalized path like Python
    if result
        .as_ref()
        .and_then(|r| r.get("shopping_results"))
        .is_none()
    {
        if let Some(normalized) = normalize_url_path(&cleaned) {
            if normalized != cleaned {
                params.insert("q".to_string(), normalized);
                result = serpapi_search(&params, client).await;
            }
        }
    }

    let result = result?;
    let shopping_results = result.get("shopping_results")?.as_array()?;
    let first = shopping_results.first()?;

    let mut data = HashMap::new();
    if let Some(title) = first.get("title").and_then(|v| v.as_str()) {
        data.insert("product_name".to_string(), serde_json::Value::String(title.to_string()));
    }
    if let Some(price) = first.get("price").or_else(|| first.get("extracted_price")) {
        data.insert("price".to_string(), price.clone());
    }

    state.merge_data(&data, "serpapi_google").await;

    // Optionally call Gemini classification on the SerpAPI title/snippet
    if let Some(title) = first.get("title").and_then(|v| v.as_str()) {
        let snippet = first.get("snippet").and_then(|v| v.as_str());
        if let Some(classified) = call_gemini_from_serpapi(url, title, snippet, client).await {
            state.merge_data(&classified, "gemini_classification").await;
        }
    }

    Some(())
}

async fn approach_serpapi_images_url(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let mut params = HashMap::new();
    params.insert("engine".to_string(), "google_images_light".to_string());
    params.insert("q".to_string(), url.to_string());
    params.insert("gl".to_string(), "us".to_string());
    params.insert("hl".to_string(), "en".to_string());
    let serp_key = env_var("SERPAPI_KEY")?;
    params.insert("api_key".to_string(), serp_key);

    let result = serpapi_search(&params, client).await?;
    let images = result.get("images_results")?.as_array()?;

    for img in images {
        let link = img.get("link").and_then(|v| v.as_str()).unwrap_or("");
        let original = img.get("original").and_then(|v| v.as_str()).unwrap_or("");
        if !link.is_empty() && !original.is_empty() && urls_match_product(url, link) {
            let mut data = HashMap::new();
            data.insert("image_urls".to_string(), serde_json::json!([original]));
            state.merge_data(&data, "serpapi_images_url").await;
            return Some(());
        }
    }

    None
}

async fn approach_serpapi_images_title(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    // Wait up to 8 seconds for product name to be available
    let mut attempts = 0;
    let product_name = loop {
        let product = state.product.lock().await;
        if let Some(name) = &product.product_name {
            break name.clone();
        }
        drop(product);

        attempts += 1;
        if attempts > 80 {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let domain = Url::parse(url).ok()?.host_str()?.to_string();
    let query = format!("\"{}\" site:{}", product_name, domain);

    let mut params = HashMap::new();
    params.insert("engine".to_string(), "google_images_light".to_string());
    params.insert("q".to_string(), query);
    params.insert("gl".to_string(), "us".to_string());
    params.insert("hl".to_string(), "en".to_string());
    let serp_key = env_var("SERPAPI_KEY")?;
    params.insert("api_key".to_string(), serp_key);

    let result = serpapi_search(&params, client).await?;
    let images = result.get("images_results")?.as_array()?;

    for img in images {
        if let Some(original) = img.get("original").and_then(|v| v.as_str()) {
            let mut data = HashMap::new();
            data.insert("image_urls".to_string(), serde_json::json!([original]));
            state.merge_data(&data, "serpapi_images_title").await;
            return Some(());
        }
    }

    None
}

async fn approach_gemini_fast(
    url: &str,
    state: &ScrapeState,
    client: &wreq::Client,
) -> Option<()> {
    let result = call_gemini_for_fast_classification(url, client).await?;
    state.merge_data(&result, "gemini_fast").await;
    Some(())
}

// ==================== MAIN ORCHESTRATOR ====================

async fn scrape_product_rust(url: String, overall_timeout_sec: f64) -> Result<ProductData, String> {
    let state = ScrapeState::new();
    println!(
        "[rust_scraper] start scrape url={} timeout_sec={}",
        url, overall_timeout_sec
    );
    let client = wreq::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?;

    let approaches = vec![
        ("gemini_fast", url.clone()),
        ("curlcffi_gemini", url.clone()),
        ("curlcffi_gemini_proxy", url.clone()),
        ("requests_gemini", url.clone()),
        ("cloudflare_gemini", url.clone()),
        ("serpapi_google", url.clone()),
        ("serpapi_images_url", url.clone()),
        ("serpapi_images_title", url.clone()),
    ];

    // Spawn all approaches concurrently
    let mut handles = Vec::new();
    for (name, url_clone) in approaches {
        let state_clone = state.clone();
        let client_clone = client.clone();

        let handle = tokio::spawn(async move {
            let span_start = Instant::now();
            println!(
                "[rust_scraper] approach {} started for url={}",
                name, url_clone
            );
            let result = match name {
                "gemini_fast" => approach_gemini_fast(&url_clone, &state_clone, &client_clone).await,
                "curlcffi_gemini" => approach_curlcffi_gemini(&url_clone, &state_clone, &client_clone).await,
                "curlcffi_gemini_proxy" => approach_curlcffi_gemini_proxy(&url_clone, &state_clone, &client_clone).await,
                "requests_gemini" => approach_requests_gemini(&url_clone, &state_clone, &client_clone).await,
                "cloudflare_gemini" => approach_cloudflare_gemini(&url_clone, &state_clone, &client_clone).await,
                "serpapi_google" => approach_serpapi_google(&url_clone, &state_clone, &client_clone).await,
                "serpapi_images_url" => approach_serpapi_images_url(&url_clone, &state_clone, &client_clone).await,
                "serpapi_images_title" => approach_serpapi_images_title(&url_clone, &state_clone, &client_clone).await,
                _ => None,
            };
            let span_elapsed = span_start.elapsed().as_millis();
            println!(
                "[rust_scraper] approach {} finished in {}ms success={}",
                name,
                span_elapsed,
                result.is_some()
            );
            (name, result)
        });
        handles.push(handle);
    }

    // Race logic: check completion every 100ms
    let timeout_duration = Duration::from_secs_f64(overall_timeout_sec);
    let race_result: Result<Result<(), ()>, _> = timeout(timeout_duration, async {
        loop {
            if state.is_complete().await {
                let elapsed = state.elapsed_ms();
                // Prefer to wait for a strong HTML+Gemini source if possible.
                if state.has_strong_source().await {
                    return Ok::<(), ()>(());
                }
                // But don't wait forever: if we've already waited > 5s with
                // no strong source, accept the best complete data available.
                if elapsed > 5000 {
                    return Ok::<(), ()>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await;

    let total_elapsed = state.elapsed_ms();
    match &race_result {
        Ok(_) => println!(
            "[rust_scraper] scrape completed in {}ms before timeout",
            total_elapsed
        ),
        Err(_) => println!(
            "[rust_scraper] scrape hit overall timeout at {}ms",
            total_elapsed
        ),
    }

    // Abort unfinished tasks
    for handle in &handles {
        handle.abort();
    }

    // Get final product data
    let product = state.product.lock().await.clone();
    let missing = product.missing_fields();
    println!(
        "[rust_scraper] final product missing_fields={:?}",
        missing
    );

    // Validate garment_type similar to Python scraper_service_v3:
    // - "unsupported" => NotFashionProductError
    // - "other" or invalid => UnsupportedProductError
    if let Some(ref gtype) = product.garment_type {
        match gtype.as_str() {
            "unsupported" => {
                return Err(format!(
                    "NotFashionProductError: The page at {} is not a fashion product page",
                    url
                ));
            }
            "other" => {
                return Err(format!(
                    "UnsupportedProductError: The product at {} is not a supported fashion item (garment_type: other)",
                    url
                ));
            }
            "upper" | "lower" | "full_body" | "shoes" => {
                // ok
            }
            _ => {
                return Err(format!(
                    "UnsupportedProductError: Could not determine garment type for product at {} (got: {})",
                    url, gtype
                ));
            }
        }
    } else {
        return Err(format!(
            "UnsupportedProductError: Could not determine garment type for product at {} (got: None)",
            url
        ));
    }

    Ok(product)
}

// ==================== PYO3 BINDINGS ====================

#[pyfunction]
#[pyo3(signature = (url, timeout_secs=None))]
fn scrape_url(py: Python, url: String, timeout_secs: Option<f64>) -> PyResult<PyObject> {
    let timeout_sec = timeout_secs.unwrap_or(30.0);

    let result = py.allow_threads(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(scrape_product_rust(url, timeout_sec))
    });
    match result {
        Ok(product) => {
            // Compute missing flags + unsupported before moving fields out of `product`
            let name_missing = product.product_name.is_none();
            let brand_missing = product.brand.is_none();
            let price_missing = product
                .price
                .as_ref()
                .and_then(|p| p.amount)
                .is_none();
            let image_missing = product.image_urls.is_empty();
            let success = !(name_missing || brand_missing || price_missing || image_missing);
            let unsupported = matches!(
                product.garment_type.as_deref(),
                Some("unsupported")
            );

            let dict = PyDict::new_bound(py);
            dict.set_item("product_name", product.product_name)?;
            dict.set_item("brand", product.brand)?;

            if let Some(price) = product.price {
                let price_dict = PyDict::new_bound(py);
                price_dict.set_item("amount", price.amount)?;
                price_dict.set_item("currency", price.currency)?;
                dict.set_item("price", price_dict)?;
            }

            dict.set_item("image_urls", product.image_urls)?;
            dict.set_item("garment_type", product.garment_type)?;
            dict.set_item("availability", product.availability)?;

            // Missing flags + success (for debugging / benchmarking)
            let missing_flags = PyDict::new_bound(py);
            missing_flags.set_item("name_missing", name_missing)?;
            missing_flags.set_item("brand_missing", brand_missing)?;
            missing_flags.set_item("price_missing", price_missing)?;
            missing_flags.set_item("image_missing", image_missing)?;
            missing_flags.set_item("unsupported", unsupported)?;
            dict.set_item("missing_flags", missing_flags)?;
            dict.set_item("success", success)?;

            Ok(dict.into())
        }
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
    }
}

#[pymodule]
fn rust_scraper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scrape_url, m)?)?;
    Ok(())
}
