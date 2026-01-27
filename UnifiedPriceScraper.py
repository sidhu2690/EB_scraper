import asyncio
import random
import os
import httpx
from urllib.parse import quote_plus
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
nest_asyncio.apply()


@dataclass
class ProductComparison:
    model_name: str = ""
    amazon_mrp: str = ""
    amazon_selling_price: str = ""
    ebazaar_mrp: str = ""
    ebazaar_selling_price: str = ""
    amazon_link: str = ""
    ebazaar_link: str = ""


class UnifiedScraper:
    def __init__(self, debug_mode: bool = False, country: str = "us"):
        self.browser = None
        self.debug_mode = debug_mode
        self.debug_dir = "debug_screenshots"
        self.country = country
        
        # Load all API keys from environment
        self.api_keys = []
        print("\n🔐 Loading API Keys...")
        for i in range(1, 11):
            key = os.environ.get(f'SCRAPER_API_KEY_{i}', '')
            if key:
                key = key.strip()  # Remove whitespace
                if len(key) >= 20:  # Valid ScraperAPI keys are 32 chars
                    self.api_keys.append(key)
                    # Show masked key for debugging
                    masked = f"{key[:6]}...{key[-4:]}" if len(key) > 10 else "***"
                    print(f"  ✓ SCRAPER_API_KEY_{i}: {masked} ({len(key)} chars)")
                else:
                    print(f"  ⚠ SCRAPER_API_KEY_{i}: Invalid (only {len(key)} chars)")
        
        self.current_key_index = 0
        self.failed_keys = set()
        self.auth_failed_keys = set()  # Track 401 errors separately
        
        if self.debug_mode:
            os.makedirs(self.debug_dir, exist_ok=True)
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]

    def _get_next_api_key(self):
        """Round-robin through available API keys, skipping failed ones"""
        if not self.api_keys:
            return None
        
        attempts = 0
        while attempts < len(self.api_keys):
            key = self.api_keys[self.current_key_index]
            key_id = self.current_key_index + 1
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            
            # Skip keys that failed authentication
            if key_id in self.auth_failed_keys:
                attempts += 1
                continue
            
            # Skip keys with exhausted quota
            if key_id in self.failed_keys:
                attempts += 1
                continue
            
            return key, key_id
        
        # All keys failed
        if self.auth_failed_keys:
            print("  ❌ All API keys failed authentication (401)")
            return None
        
        print("  ⚠ All API keys exhausted, resetting...")
        self.failed_keys.clear()
        return self.api_keys[0], 1

    def _mark_key_auth_failed(self, key_id: int):
        """Mark API key as having authentication failure"""
        self.auth_failed_keys.add(key_id)
        print(f"  ❌ API Key #{key_id} authentication failed (invalid key)")

    def _mark_key_quota_exceeded(self, key_id: int):
        """Mark API key as quota exceeded"""
        self.failed_keys.add(key_id)
        print(f"  ⚠ API Key #{key_id} quota exceeded")

    async def run(self, input_csv: str) -> pd.DataFrame:
        df_input = pd.read_csv(input_csv)
        products = []
        
        print(f"\n🔑 Total valid API keys: {len(self.api_keys)}")
        print(f"🌍 Region: {self.country.upper()}")
        
        if not self.api_keys:
            print("\n" + "=" * 60)
            print("❌ ERROR: No valid API keys found!")
            print("=" * 60)
            print("Please check your GitHub Secrets:")
            print("  1. Go to Repository → Settings → Secrets → Actions")
            print("  2. Verify SCRAPER_API_KEY_1, SCRAPER_API_KEY_2, etc. exist")
            print("  3. Make sure keys have no extra spaces/newlines")
            print("  4. Test your key at: https://dashboard.scraperapi.com")
            print("=" * 60 + "\n")
        
        async with async_playwright() as p:
            await self._setup_browser(p)
            
            total = len(df_input)
            for idx, row in df_input.iterrows():
                print(f"\n[{idx + 1}/{total}] Processing: {row['model_name']}")
                
                product = ProductComparison(
                    model_name=row['model_name'],
                    amazon_link=row.get('amazon_link', ''),
                    ebazaar_link=row.get('ebazaar_link', '')
                )
                
                if pd.notna(row.get('amazon_link')) and str(row['amazon_link']).strip():
                    mrp, selling = await self._scrape_amazon(row['amazon_link'], idx)
                    product.amazon_mrp = mrp
                    product.amazon_selling_price = selling
                
                await asyncio.sleep(random.uniform(1, 2))
                
                if pd.notna(row.get('ebazaar_link')) and str(row['ebazaar_link']).strip():
                    mrp, selling = await self._scrape_ebazaar(row['ebazaar_link'], idx)
                    product.ebazaar_mrp = mrp
                    product.ebazaar_selling_price = selling
                
                products.append(product)
                print(f"  ✓ Done: MRP(A)={product.amazon_mrp}, SP(A)={product.amazon_selling_price}, "
                      f"MRP(E)={product.ebazaar_mrp}, SP(E)={product.ebazaar_selling_price}")
                
                if idx < total - 1:
                    await asyncio.sleep(random.uniform(2, 4))
            
            await self.browser.close()
        
        return pd.DataFrame([asdict(p) for p in products])

    async def _setup_browser(self, playwright):
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
            ]
        )

    async def _scrape_amazon(self, url: str, idx: int) -> tuple:
        """Scrape Amazon using ScraperAPI"""
        
        if not self.api_keys:
            return "No API Key", "No API Key"
        
        # Check if all keys failed auth
        if len(self.auth_failed_keys) == len(self.api_keys):
            return "Invalid API Keys", "Invalid API Keys"
        
        max_retries = min(3, len(self.api_keys))
        
        for attempt in range(max_retries):
            result = self._get_next_api_key()
            if result is None:
                return "No Valid API Key", "No Valid API Key"
            
            api_key, key_id = result
            
            try:
                # URL encode the target URL properly
                encoded_url = quote_plus(url)
                
                # Build API URL
                api_url = (
                    f"http://api.scraperapi.com"
                    f"?api_key={api_key}"
                    f"&url={encoded_url}"
                    f"&render=true"
                    f"&country_code={self.country}"
                )
                
                print(f"  → Using API Key #{key_id} ({self.country.upper()} region)...")
                
                async with httpx.AsyncClient(timeout=90) as client:
                    response = await client.get(api_url)
                    
                    # Handle different error codes
                    if response.status_code == 401:
                        # 401 = Invalid API key
                        print(f"  ❌ API Key #{key_id}: 401 Unauthorized (invalid key)")
                        self._mark_key_auth_failed(key_id)
                        continue
                    
                    if response.status_code == 403:
                        # 403 = Forbidden (might be quota or invalid)
                        error_text = response.text[:200]
                        print(f"  ⚠ API Key #{key_id}: 403 Forbidden - {error_text}")
                        self._mark_key_quota_exceeded(key_id)
                        continue
                    
                    if response.status_code == 429:
                        # 429 = Rate limited
                        print(f"  ⚠ API Key #{key_id}: 429 Rate Limited")
                        self._mark_key_quota_exceeded(key_id)
                        continue
                    
                    if response.status_code == 500:
                        print(f"  ⚠ ScraperAPI server error, retrying...")
                        await asyncio.sleep(2)
                        continue
                    
                    if response.status_code != 200:
                        print(f"  ⚠ ScraperAPI returned {response.status_code}")
                        continue
                    
                    html = response.text
                    
                    # Save HTML for debugging
                    if self.debug_mode:
                        debug_file = f"{self.debug_dir}/amazon_{idx}_{key_id}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(html)
                    
                    # Check for bot detection
                    if 'captcha' in html.lower() or 'robot' in html.lower():
                        print("  ⚠ CAPTCHA/Bot detection, trying next key...")
                        continue
                    
                    mrp, selling = self._parse_amazon_prices(html)
                    
                    if selling == "N/A" and attempt < max_retries - 1:
                        print("  ⚠ No price found, trying another key...")
                        continue
                    
                    print(f"  Amazon: MRP={mrp}, Selling={selling}")
                    return mrp, selling
                    
            except httpx.TimeoutException:
                print(f"  ⚠ Timeout on attempt {attempt + 1}")
                continue
            except Exception as e:
                print(f"  ⚠ Attempt {attempt + 1} failed: {str(e)[:50]}")
                continue
        
        return "Error", "Error"

    def _parse_amazon_prices(self, html: str) -> tuple:
        """Parse Amazon prices from HTML"""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        
        selling_price = "N/A"
        mrp = "N/A"
        
        selling_selectors = [
            '.priceToPay .a-offscreen',
            '#corePrice_feature_div .a-offscreen',
            '.a-price:not([data-a-strike="true"]) .a-offscreen',
            '#priceblock_ourprice',
            '#priceblock_dealprice',
            '#priceblock_saleprice',
            'span[data-a-color="price"] .a-offscreen',
        ]
        
        for selector in selling_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text and '$' in text:
                    parent = el.find_parent(attrs={'data-a-strike': 'true'})
                    if not parent:
                        parent = el.find_parent(class_='a-text-price')
                        if not parent or 'data-a-strike' not in str(parent):
                            selling_price = text
                            break
        
        mrp_selectors = [
            '.basisPrice .a-offscreen',
            '.a-text-price[data-a-strike="true"] .a-offscreen',
            '.a-text-price .a-offscreen',
            '[data-a-strike="true"] .a-offscreen',
            '#priceblock_listprice',
        ]
        
        for selector in mrp_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text and '$' in text:
                    mrp = text
                    break
        
        if mrp == "N/A" and selling_price != "N/A":
            mrp = selling_price
        
        return mrp, selling_price

    async def _scrape_ebazaar(self, url: str, idx: int) -> tuple:
        """Scrape eBazaar using Playwright (FREE)"""
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=random.choice(self.user_agents),
        )
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))
            
            data = await page.evaluate('''() => {
                let sellingPrice = '';
                let mrp = '';
                
                const finalPriceEl = document.querySelector('[data-price-type="finalPrice"]');
                if (finalPriceEl) {
                    const amount = finalPriceEl.getAttribute('data-price-amount');
                    if (amount) sellingPrice = '$' + parseFloat(amount).toFixed(2);
                }
                
                const oldPriceEl = document.querySelector('[data-price-type="oldPrice"]');
                if (oldPriceEl) {
                    const amount = oldPriceEl.getAttribute('data-price-amount');
                    if (amount) mrp = '$' + parseFloat(amount).toFixed(2);
                }
                
                if (!sellingPrice || !mrp) {
                    const containers = document.querySelectorAll('.product-info-price, .price-box');
                    for (const container of containers) {
                        const prices = container.innerText.match(/\\$[\\d,]+\\.?\\d*/g);
                        if (prices && prices.length >= 2) {
                            const nums = prices.map(p => parseFloat(p.replace(/[$,]/g, '')));
                            if (!sellingPrice) sellingPrice = '$' + Math.min(...nums).toFixed(2);
                            if (!mrp) mrp = '$' + Math.max(...nums).toFixed(2);
                            break;
                        } else if (prices && prices.length === 1 && !sellingPrice) {
                            sellingPrice = prices[0];
                        }
                    }
                }
                
                if (!mrp && sellingPrice) mrp = sellingPrice;
                
                return { mrp, sellingPrice };
            }''')
            
            mrp = data.get('mrp', '').strip() or "N/A"
            selling = data.get('sellingPrice', '').strip() or "N/A"
            
            print(f"  eBazaar: MRP={mrp}, Selling={selling}")
            return mrp, selling
            
        except Exception as e:
            print(f"  ✗ eBazaar error: {str(e)[:80]}")
            return "Error", "Error"
        finally:
            await context.close()


def main():
    print("=" * 60)
    print("Price Scraper Started")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    scraper = UnifiedScraper(debug_mode=True, country="us")
    
    try:
        df_output = asyncio.run(scraper.run('price/input_links.csv'))
        df_output.to_csv("price/data.csv", index=False)
        
        print("\n" + "=" * 60)
        print(f"✓ Saved {len(df_output)} products to price/data.csv")
        
        amazon_ok = len(df_output[~df_output['amazon_selling_price'].isin(['N/A', 'Error', 'CAPTCHA', 'Blocked', '', 'No API Key', 'Invalid API Keys', 'No Valid API Key'])])
        ebazaar_ok = len(df_output[~df_output['ebazaar_selling_price'].isin(['N/A', 'Error', ''])])
        
        print(f"  Amazon success:  {amazon_ok}/{len(df_output)}")
        print(f"  eBazaar success: {ebazaar_ok}/{len(df_output)}")
        print("=" * 60)
        
    except FileNotFoundError:
        print("✗ Error: 'price/input_links.csv' not found!")
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
