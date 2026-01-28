import asyncio
import random
import os
import httpx
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio
nest_asyncio.apply()


@dataclass
class ProductComparison:
    model_name: str = ""
    amazon_method: str = ""  # A=Request, B=Playwright, C=API
    amazon_mrp: str = ""
    amazon_selling_price: str = ""
    ebazaar_mrp: str = ""
    ebazaar_selling_price: str = ""
    amazon_link: str = ""
    ebazaar_link: str = ""


class UnifiedScraper:
    def __init__(self, debug_mode: bool = False):
        self.browser = None
        self.debug_mode = debug_mode
        self.debug_dir = "debug_screenshots"
        
        # Load all API keys from environment
        self.api_keys = []
        for i in range(1, 8):
            key = os.environ.get(f'SCRAPER_API_KEY_{i}', '')
            if key:
                self.api_keys.append(key)
        
        self.current_key_index = 0
        self.failed_keys = set()
        
        if self.debug_mode:
            os.makedirs(self.debug_dir, exist_ok=True)
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]

    def _get_next_api_key(self):
        """Round-robin through available API keys"""
        if not self.api_keys:
            return None
        
        attempts = 0
        while attempts < len(self.api_keys):
            key = self.api_keys[self.current_key_index]
            key_id = self.current_key_index + 1
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            
            if key_id in self.failed_keys:
                attempts += 1
                continue
            
            return key, key_id
        
        print("  ⚠ All API keys exhausted, resetting...")
        self.failed_keys.clear()
        key = self.api_keys[0]
        self.current_key_index = 1 % len(self.api_keys)
        return key, 1

    def _mark_key_failed(self, key_id: int):
        self.failed_keys.add(key_id)
        print(f"  ⚠ API Key #{key_id} marked as exhausted")

    def _is_valid_price(self, mrp: str, selling: str) -> bool:
        """Check if we got valid prices"""
        invalid = ['N/A', 'Error', 'CAPTCHA', 'Blocked', '', 'No API Key', None]
        return selling not in invalid and mrp not in invalid

    async def run(self, input_csv: str) -> pd.DataFrame:
        df_input = pd.read_csv(input_csv)
        products = []
        
        print(f"\n🔑 Loaded {len(self.api_keys)} API keys (used as fallback)")
        
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
                    mrp, selling, method = await self._scrape_amazon(row['amazon_link'], idx)
                    product.amazon_mrp = mrp
                    product.amazon_selling_price = selling
                    product.amazon_method = method
                
                await asyncio.sleep(random.uniform(1, 2))
                
                if pd.notna(row.get('ebazaar_link')) and str(row['ebazaar_link']).strip():
                    mrp, selling = await self._scrape_ebazaar(row['ebazaar_link'], idx)
                    product.ebazaar_mrp = mrp
                    product.ebazaar_selling_price = selling
                
                products.append(product)
                print(f"  ✓ Done: Method={product.amazon_method}, MRP(A)={product.amazon_mrp}, SP(A)={product.amazon_selling_price}, "
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
        """Scrape Amazon - tries: 1) Direct request, 2) Playwright, 3) ScraperAPI
        Returns: (mrp, selling_price, method)
        Method: A=Request, B=Playwright, C=API, X=Failed
        """
        
        # Method A: Direct request with httpx + BeautifulSoup (FREE)
        print("  → Method A: Direct request...")
        mrp, selling = await self._scrape_amazon_direct(url)
        if self._is_valid_price(mrp, selling):
            print(f"  ✓ Direct request success!")
            return mrp, selling, "A"
        
        # Method B: Playwright (FREE)
        print("  → Method B: Playwright...")
        mrp, selling = await self._scrape_amazon_playwright(url, idx)
        if self._is_valid_price(mrp, selling):
            print(f"  ✓ Playwright success!")
            return mrp, selling, "B"
        
        # Method C: ScraperAPI (PAID - last resort)
        print("  → Method C: ScraperAPI (fallback)...")
        mrp, selling = await self._scrape_amazon_api(url, idx)
        if self._is_valid_price(mrp, selling):
            return mrp, selling, "C"
        
        # All methods failed
        return mrp, selling, "X"

    async def _scrape_amazon_direct(self, url: str) -> tuple:
        """Try direct HTTP request with BeautifulSoup"""
        try:
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    html = response.text
                    if 'captcha' in html.lower() or 'robot' in html.lower():
                        print("  ⚠ Direct: CAPTCHA/Bot detected")
                        return "CAPTCHA", "CAPTCHA"
                    return self._parse_amazon_prices(html)
                else:
                    print(f"  ⚠ Direct: Status {response.status_code}")
        except Exception as e:
            print(f"  ⚠ Direct request failed: {str(e)[:50]}")
        return "N/A", "N/A"

    async def _scrape_amazon_playwright(self, url: str, idx: int) -> tuple:
        """Try Playwright browser"""
        context = None
        try:
            context = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=random.choice(self.user_agents),
            )
            page = await context.new_page()
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))
            
            html = await page.content()
            if 'captcha' in html.lower() or 'robot' in html.lower():
                print("  ⚠ Playwright: CAPTCHA/Bot detected")
                return "CAPTCHA", "CAPTCHA"
            
            return self._parse_amazon_prices(html)
        except Exception as e:
            print(f"  ⚠ Playwright failed: {str(e)[:50]}")
            return "N/A", "N/A"
        finally:
            if context:
                await context.close()

    async def _scrape_amazon_api(self, url: str, idx: int) -> tuple:
        """Scrape Amazon using ScraperAPI (PAID - last resort)"""
        
        if not self.api_keys:
            print("  ⚠ No API keys available")
            return "No API Key", "No API Key"
        
        max_retries = min(3, len(self.api_keys))
        
        for attempt in range(max_retries):
            result = self._get_next_api_key()
            if result is None:
                return "No API Key", "No API Key"
            
            api_key, key_id = result
            
            try:
                api_url = f"http://api.scraperapi.com?api_key={api_key}&url={url}&render=true"
                print(f"  → Using API Key #{key_id}...")
                
                async with httpx.AsyncClient(timeout=90) as client:
                    response = await client.get(api_url)
                    
                    if response.status_code in [403, 429]:
                        print(f"  ⚠ API Key #{key_id} quota exceeded")
                        self._mark_key_failed(key_id)
                        continue
                    
                    if response.status_code != 200:
                        print(f"  ⚠ ScraperAPI returned {response.status_code}")
                        continue
                    
                    html = response.text
                    
                    if 'captcha' in html.lower():
                        print("  ⚠ CAPTCHA detected")
                        return "CAPTCHA", "CAPTCHA"
                    
                    mrp, selling = self._parse_amazon_prices(html)
                    print(f"  Amazon: MRP={mrp}, Selling={selling}")
                    return mrp, selling
                    
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
            '.a-price:not([data-a-strike="true"]) .a-offscreen',
            '.priceToPay .a-offscreen',
            '#priceblock_ourprice',
            '#priceblock_dealprice',
            '.a-price .a-offscreen',
        ]
        
        for selector in selling_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text and ('$' in text or '₹' in text):
                    parent = el.find_parent(attrs={'data-a-strike': 'true'})
                    if not parent:
                        parent = el.find_parent(class_='a-text-price')
                        if not parent:
                            selling_price = text
                            break
        
        mrp_selectors = [
            '.a-text-price .a-offscreen',
            '.basisPrice .a-offscreen',
            '[data-a-strike="true"] .a-offscreen',
            '#priceblock_listprice',
        ]
        
        for selector in mrp_selectors:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text and ('$' in text or '₹' in text):
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
    print("Method Legend: A=Request, B=Playwright, C=API, X=Failed")
    print("=" * 60)
    
    scraper = UnifiedScraper(debug_mode=True)
    
    try:
        df_output = asyncio.run(scraper.run('price/input_links.csv'))
        df_output.to_csv("price/data.csv", index=False)
        
        print("\n" + "=" * 60)
        print(f"✓ Saved {len(df_output)} products to price/data.csv")
        
        amazon_ok = len(df_output[~df_output['amazon_selling_price'].isin(['N/A', 'Error', 'CAPTCHA', 'Blocked', '', 'No API Key'])])
        ebazaar_ok = len(df_output[~df_output['ebazaar_selling_price'].isin(['N/A', 'Error', ''])])
        
        # Method breakdown
        method_counts = df_output['amazon_method'].value_counts()
        print(f"\n  Method breakdown:")
        print(f"    A (Request):    {method_counts.get('A', 0)}")
        print(f"    B (Playwright): {method_counts.get('B', 0)}")
        print(f"    C (API):        {method_counts.get('C', 0)}")
        print(f"    X (Failed):     {method_counts.get('X', 0)}")
        
        print(f"\n  Amazon success:  {amazon_ok}/{len(df_output)}")
        print(f"  eBazaar success: {ebazaar_ok}/{len(df_output)}")
        print("=" * 60)
        
    except FileNotFoundError:
        print("✗ Error: 'price/input_links.csv' not found!")
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
