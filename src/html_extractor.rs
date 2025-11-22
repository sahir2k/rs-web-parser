use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde_json::{json, Map, Value};
use url::Url;

pub struct ProductDataExtractor {
    max_tokens: usize,
    token_char_ratio: usize,
}

impl ProductDataExtractor {
    pub fn new(max_tokens: usize) -> Self {
        Self {
            max_tokens,
            token_char_ratio: 4,
        }
    }

    pub fn extract_product_data(&self, url: &str, html: &str) -> Value {
        let document = Html::parse_document(html);

        let structured_data = self.extract_structured_data(&document);
        let inline_json_images = self.extract_inline_json(&document);

        let price_signals = self.extract_price_signals(&document);
        let text_content = self.extract_text_content(&document);

        let mut all_images: Vec<String> = Vec::new();

        // Method 1: smart filtering from <img> tags
        let img_tag_images = self.filter_product_images(&document, url);
        all_images.extend(img_tag_images.into_iter().map(|img| img.src));

        // Method 2: JSON-LD images
        let json_ld_images = self.flatten_json_ld_images(&structured_data);
        all_images.extend(json_ld_images);

        // Method 3: inline JSON images
        all_images.extend(inline_json_images);

        // Method 4: preload images
        let preload_images = self.extract_preload_images(&document, url);
        all_images.extend(preload_images);

        // Deduplicate while preserving order
        let mut seen = std::collections::HashSet::new();
        let mut unique_images = Vec::new();
        for img_url in all_images {
            if !img_url.is_empty() && !seen.contains(&img_url) {
                seen.insert(img_url.clone());
                unique_images.push(img_url);
            }
        }

        // Convert to list of dicts with src and metadata (alt/score left empty)
        let images: Vec<Value> = unique_images
            .into_iter()
            .map(|u| json!({ "src": u, "alt": "", "score": 0 }))
            .collect();

        let mut output = Map::new();
        output.insert("url".to_string(), Value::String(url.to_string()));
        output.insert("structured_data".to_string(), structured_data);
        output.insert("price_signals".to_string(), Value::Array(price_signals.into_iter().map(Value::String).collect()));
        output.insert("images".to_string(), Value::Array(images));
        output.insert("content".to_string(), text_content);

        let mut output_value = Value::Object(output);
        let mut output_str = serde_json::to_string(&output_value).unwrap_or_default();
        let mut estimated_tokens = self.estimate_tokens(&output_str);

        if estimated_tokens > self.max_tokens {
            self.trim_content(&mut output_value);
            output_str = serde_json::to_string(&output_value).unwrap_or_default();
            estimated_tokens = self.estimate_tokens(&output_str);
        }

        output_value
    }

    fn estimate_tokens(&self, text: &str) -> usize {
        text.len() / self.token_char_ratio
    }

    fn extract_structured_data(&self, document: &Html) -> Value {
        let mut json_ld_arr: Vec<Value> = Vec::new();
        let mut meta_tags = Map::new();
        let mut open_graph = Map::new();
        let mut twitter_card = Map::new();

        let script_sel = Selector::parse("script[type='application/ld+json']").unwrap();
        for script in document.select(&script_sel) {
            let text = script.text().collect::<String>();
            if text.trim().is_empty() {
                continue;
            }
            if let Ok(data) = serde_json::from_str::<Value>(&text) {
                if let Some(obj) = data.as_object() {
                    if let Some(t) = obj.get("@type").and_then(|v| v.as_str()) {
                        if matches!(t, "Product" | "Offer" | "AggregateOffer" | "ProductGroup") {
                            json_ld_arr.push(Value::Object(obj.clone()));
                        }
                    } else if let Some(graph) = obj.get("@graph").and_then(|v| v.as_array()) {
                        for item in graph {
                            if let Some(item_obj) = item.as_object() {
                                if let Some(t) = item_obj.get("@type").and_then(|v| v.as_str()) {
                                    if matches!(t, "Product" | "Offer" | "ProductGroup") {
                                        json_ld_arr.push(Value::Object(item_obj.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let meta_sel = Selector::parse("meta").unwrap();
        for meta in document.select(&meta_sel) {
            let value = meta.value();
            let name = value
                .attr("name")
                .or_else(|| value.attr("property"))
                .unwrap_or("");
            let content = value.attr("content").unwrap_or("");
            if name.is_empty() || content.is_empty() {
                continue;
            }
            if name.starts_with("og:") {
                open_graph.insert(name.to_string(), Value::String(content.to_string()));
            } else if name.starts_with("twitter:") {
                twitter_card.insert(name.to_string(), Value::String(content.to_string()));
            } else if name.to_lowercase().contains("product") || name.to_lowercase().contains("price") {
                meta_tags.insert(name.to_string(), Value::String(content.to_string()));
            }
        }

        json!({
            "json_ld": json_ld_arr,
            "meta_tags": meta_tags,
            "open_graph": open_graph,
            "twitter_card": twitter_card,
        })
    }

    fn extract_inline_json(&self, document: &Html) -> Vec<String> {
        let mut images = Vec::new();
        let script_sel = Selector::parse("script").unwrap();
        let json_indicators = [
            "window.INITIAL_STATE",
            "window.__INITIAL_DATA__",
            "window.__NEXT_DATA__",
            "window.__PRODUCT_DATA__",
            "__INITIAL_STATE__",
        ];

        let patterns = [
            r#""images?"\s*:\s*\[([^\]]+)\]"#,
            r#""imageUrls?"\s*:\s*\[([^\]]+)\]"#,
            r#""img"\s*:\s*\[([^\]]+)\]"#,
        ];
        let regexes: Vec<Regex> = patterns
            .iter()
            .map(|p| Regex::new(p).unwrap())
            .collect();
        let url_re = Regex::new(r#"https?://[^"']+\.(?:jpg|jpeg|png|webp)"#).unwrap();

        for script in document.select(&script_sel) {
            let value = script.value();
            if value.attr("type").is_some() {
                continue;
            }

            let script_content = script.text().collect::<String>();
            if script_content.len() < 500 {
                continue;
            }

            if !json_indicators.iter().any(|ind| script_content.contains(ind)) {
                continue;
            }

            for re_pat in &regexes {
                for caps in re_pat.captures_iter(&script_content) {
                    if let Some(m) = caps.get(1) {
                        let mut array_content = m.as_str().to_string();
                        // best-effort unicode escape handling
                        if let Ok(decoded) = serde_json::from_str::<String>(&format!("\"{}\"", array_content)) {
                            array_content = decoded;
                        }
                        for m in url_re.find_iter(&array_content) {
                            let mut url = m.as_str().to_string();
                            if url.contains("__IMAGE_PARAMS__") {
                                url = url.replace("__IMAGE_PARAMS__", "f_auto");
                            }
                            images.push(url);
                        }
                    }
                }
            }
        }

        images
    }

    fn flatten_json_ld_images(&self, structured_data: &Value) -> Vec<String> {
        let mut images = Vec::new();
        let json_ld_arr = structured_data
            .get("json_ld")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for json_ld in json_ld_arr {
            if let Some(obj) = json_ld.as_object() {
                if let Some(image_field) = obj.get("image") {
                    match image_field {
                        Value::Array(items) => {
                            for item in items {
                                match item {
                                    Value::Object(o) => {
                                        if let Some(u) = o
                                            .get("contentUrl")
                                            .and_then(|v| v.as_str())
                                            .or_else(|| o.get("url").and_then(|v| v.as_str()))
                                        {
                                            images.push(u.to_string());
                                        }
                                    }
                                    Value::String(s) => images.push(s.to_string()),
                                    _ => {}
                                }
                            }
                        }
                        Value::String(s) => images.push(s.to_string()),
                        Value::Object(o) => {
                            if let Some(u) = o
                                .get("contentUrl")
                                .and_then(|v| v.as_str())
                                .or_else(|| o.get("url").and_then(|v| v.as_str()))
                            {
                                images.push(u.to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        images
    }

    fn extract_preload_images(&self, document: &Html, base_url: &str) -> Vec<String> {
        let mut images = Vec::new();
        let link_sel = Selector::parse("link[rel='preload']").unwrap();
        let img_ext_re = Regex::new(r"\.(jpg|jpeg|png|webp)$").unwrap();

        for link in document.select(&link_sel) {
            let value = link.value();
            let as_attr = value.attr("as").unwrap_or("");
            let href = value.attr("href").unwrap_or("");
            if as_attr == "image" && !href.is_empty() {
                if let Ok(base) = Url::parse(base_url) {
                    if let Ok(full) = base.join(href) {
                        let full_url = full.to_string();
                        if img_ext_re.is_match(&full_url.to_lowercase()) {
                            images.push(full_url);
                        }
                    }
                }
            }
        }
        images
    }

    fn extract_price_signals(&self, document: &Html) -> Vec<String> {
        let mut price_signals: Vec<String> = Vec::new();
        let price_regex =
            Regex::new(r#"[\$£€¥₹]\s*[\d,]+\.?\d*\+?|\d+[\.,]\d+\s*(?:USD|EUR|GBP|INR|CAD|AUD)"#)
                .unwrap();
        let whitespace_re = Regex::new(r"\s+").unwrap();

        let selectors = [
            "[class*=\"price\"]",
            "[id*=\"price\"]",
            "[data-price]",
            "[itemprop=\"price\"]",
            "span",
            "div",
            "p",
        ];

        for sel_str in &selectors {
            let sel = match Selector::parse(sel_str) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut count = 0;
            for elem in document.select(&sel) {
                if count >= 20 {
                    break;
                }
                let text = elem.text().collect::<String>().trim().to_string();
                if !text.is_empty() && price_regex.is_match(&text) {
                    let cleaned = whitespace_re.replace_all(&text, " ").to_string();
                    price_signals.push(cleaned.chars().take(100).collect());
                    count += 1;
                }
            }
        }

        let mut seen = std::collections::HashSet::new();
        let mut unique = Vec::new();
        for price in price_signals {
            if !seen.contains(&price) {
                seen.insert(price.clone());
                unique.push(price);
                if unique.len() >= 10 {
                    break;
                }
            }
        }
        unique
    }

    fn extract_text_content(&self, document: &Html) -> Value {
        let mut title = String::new();
        let mut headings = Vec::new();
        let mut breadcrumbs = Vec::new();
        let mut descriptions = Vec::new();
        let mut specifications = Vec::new();

        // title
        if let Ok(sel) = Selector::parse("title") {
            if let Some(elem) = document.select(&sel).next() {
                title = elem.text().collect::<String>().trim().to_string();
            }
        }

        // h1, h2 headings
        if let Ok(sel) = Selector::parse("h1, h2") {
            for elem in document.select(&sel).take(5) {
                let text = elem.text().collect::<String>().trim().to_string();
                if !text.is_empty() && text.len() < 200 {
                    headings.push(text);
                }
            }
        }

        // breadcrumbs
        let breadcrumb_selectors = [
            "[class*=\"breadcrumb\"]",
            "[id*=\"breadcrumb\"]",
            "[itemtype*=\"BreadcrumbList\"]",
            "nav",
        ];
        for sel_str in &breadcrumb_selectors {
            if let Ok(sel) = Selector::parse(sel_str) {
                for elem in document.select(&sel).take(2) {
                    let text = elem.text().collect::<Vec<_>>().join(" ");
                    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    if !text.is_empty() && text.len() < 300 {
                        breadcrumbs.push(text);
                        break;
                    }
                }
            }
        }

        // descriptions
        let desc_selectors = [
            "[class*=\"description\"]",
            "[id*=\"description\"]",
            "[itemprop=\"description\"]",
            "[class*=\"product-info\"]",
            "[class*=\"product-detail\"]",
        ];
        for sel_str in &desc_selectors {
            if let Ok(sel) = Selector::parse(sel_str) {
                for elem in document.select(&sel).take(3) {
                    let text = elem.text().collect::<Vec<_>>().join(" ");
                    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    let len = text.len();
                    if !text.is_empty() && len > 20 && len < 1000 {
                        descriptions.push(text);
                    }
                }
            }
        }

        // specifications
        let spec_selectors = [
            "[class*=\"spec\"]",
            "[class*=\"feature\"]",
            "[class*=\"attribute\"]",
            "table",
            "dl",
        ];
        for sel_str in &spec_selectors {
            if let Ok(sel) = Selector::parse(sel_str) {
                for elem in document.select(&sel).take(3) {
                    let text = elem.text().collect::<Vec<_>>().join(" ");
                    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
                    let len = text.len();
                    if !text.is_empty() && len > 10 && len < 500 {
                        specifications.push(text);
                    }
                }
            }
        }

        json!({
            "title": title,
            "headings": headings,
            "breadcrumbs": breadcrumbs,
            "descriptions": descriptions,
            "specifications": specifications,
        })
    }

    fn filter_product_images(&self, document: &Html, base_url: &str) -> Vec<ImageInfo> {
        let mut images = Vec::new();

        let excluded_patterns = [
            "logo", "icon", "favicon", "sprite", "loading", "placeholder",
            "social", "facebook", "twitter", "instagram", "youtube",
            "payment", "visa", "mastercard", "paypal", "stripe",
            "shipping", "delivery", "banner", "advertisement",
        ];

        // Walk images
        if let Ok(img_sel) = Selector::parse("img") {
            for img in document.select(&img_sel).take(50) {
                let value = img.value();
                let mut src = value
                    .attr("src")
                    .or_else(|| value.attr("data-src"))
                    .or_else(|| value.attr("data-lazy-src"))
                    .unwrap_or("")
                    .to_string();

                if src.is_empty() {
                    continue;
                }

                // resolve relative URL
                if let Ok(base) = Url::parse(base_url) {
                    if let Ok(full) = base.join(&src) {
                        src = full.to_string();
                    }
                }

                let src_lower = src.to_lowercase();
                if excluded_patterns.iter().any(|p| src_lower.contains(p)) {
                    continue;
                }

                // rough size check
                if let (Some(w), Some(h)) = (value.attr("width"), value.attr("height")) {
                    if let (Ok(w), Ok(h)) = (w.replace("px", "").parse::<i32>(), h.replace("px", "").parse::<i32>()) {
                        if w < 100 || h < 100 {
                            continue;
                        }
                    }
                }

                let alt = value.attr("alt").unwrap_or("").to_string();
                let title = value.attr("title").unwrap_or("").to_string();

                let mut score = 0;
                if src_lower.contains("product") || src_lower.contains("item") || src_lower.contains("gallery") {
                    score += 2;
                }
                if !alt.is_empty() && alt.len() > 10 {
                    score += 2;
                }
                if ["cdn", "media", "assets", "images"].iter().any(|p| src_lower.contains(p)) {
                    score += 1;
                }
                if value.attr("itemprop") == Some("image") {
                    score += 3;
                }

                // parent class heuristics (up 3 levels)
                let mut parent_opt = img.parent();
                for _ in 0..3 {
                    if let Some(parent) = parent_opt.and_then(ElementRef::wrap) {
                        let class_attr = parent.value().attr("class").unwrap_or("");
                        if class_attr.to_lowercase().contains("product")
                            || class_attr.to_lowercase().contains("gallery")
                        {
                            score += 2;
                            break;
                        }
                        parent_opt = parent.parent();
                    } else {
                        break;
                    }
                }

                if score >= 2 {
                    images.push(ImageInfo { src, alt, score });
                }
            }
        }

        images.sort_by(|a, b| b.score.cmp(&a.score));
        images.truncate(15);
        images
    }

    fn trim_content(&self, value: &mut Value) {
        if let Some(obj) = value.as_object_mut() {
            if let Some(content) = obj.get_mut("content") {
                if let Some(cobj) = content.as_object_mut() {
                    if let Some(desc) = cobj.get_mut("descriptions") {
                        if let Some(arr) = desc.as_array_mut() {
                            if arr.len() > 2 {
                                arr.truncate(2);
                            }
                        }
                    }
                    if let Some(specs) = cobj.get_mut("specifications") {
                        if let Some(arr) = specs.as_array_mut() {
                            if arr.len() > 2 {
                                arr.truncate(2);
                            }
                        }
                    }
                }
            }
            if let Some(images) = obj.get_mut("images") {
                if let Some(arr) = images.as_array_mut() {
                    if arr.len() > 8 {
                        arr.truncate(8);
                    }
                }
            }
        }
    }
}

struct ImageInfo {
    src: String,
    alt: String,
    score: i32,
}
