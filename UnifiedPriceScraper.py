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
        
        # ScraperAPI key from environment
        self.scraper_api_key = os.environ.get('SCRAPER_API_KEY', '')
        
        if self.debug_mode:
            os.makedirs(self.debug_dir, exist_ok=True)
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]

    async def run(self, input_csv: str) -> pd.DataFrame:
        df_input = pd.read_csv(input_csv)
        products = []
        
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
                
                # Scrape Amazon using ScraperAPI
                if pd.notna(row.get('amazon_link')) and str(row['amazon_link']).strip():
                    mrp, selling = await self._scrape_amazon(row['amazon_link'], idx)
                    product.amazon_mrp = mrp
                    product.amazon_selling_price = selling
                
                await asyncio.sleep(random.uniform(1, 2))
                
                # Scrape eBazaar using Playwright
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
        """Scrape Amazon using ScraperAPI to bypass CAPTCHA"""
        
        if not self.scraper_api_key:
            print("  ⚠ No SCRAPER_API_KEY found, trying direct...")
            return await self._scrape_amazon_direct(url, idx)
        
        try:
            # ScraperAPI endpoint
            api_url = f"http://api.scraperapi.com?api_key={self.scraper_api_key}&url={url}&render=true"
            
            print("  → Fetching via ScraperAPI...")
            
            async with httpx.AsyncClient(timeout=90) as client:
                response = await client.get(api_url)
                
                if response.status_code != 200:
                    print(f"  ⚠ ScraperAPI returned {response.status_code}")
                    return "Error", "Error"
                
                html = response.text
                
                # Check for CAPTCHA
                if 'captcha' in html.lower():
                    print("  ⚠ CAPTCHA still detected")
                    return "CAPTCHA", "CAPTCHA"
                
                # Parse prices from HTML
                mrp, selling = self._parse_amazon_prices(html)
                
                print(f"  Amazon: MRP={mrp}, Selling={selling}")
                return mrp, selling
                
        except Exception as e:
            print(f"  ✗ Amazon error: {str(e)[:80]}")
            return "Error", "Error"

    def _parse_amazon_prices(self, html: str) -> tuple:
        """Parse Amazon prices from HTML"""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        
        selling_price = "N/A"
        mrp = "N/A"
        
        # Selling price selectors
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
                    # Check it's not a struck-through price
                    parent = el.find_parent(attrs={'data-a-strike': 'true'})
                    if not parent:
                        parent = el.find_parent(class_='a-text-price')
                        if not parent:
                            selling_price = text
                            break
        
        # MRP selectors
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
        
        # If no MRP, use selling price
        if mrp == "N/A" and selling_price != "N/A":
            mrp = selling_price
        
        return mrp, selling_price

    async def _scrape_amazon_direct(self, url: str, idx: int) -> tuple:
        """Fallback: Direct scraping (likely to get CAPTCHA)"""
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=random.choice(self.user_agents),
        )
        page = await context.new_page()
        
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await asyncio.sleep(3)
            
            content = await page.content()
            if 'captcha' in content.lower():
                if self.debug_mode:
                    await self._save_debug(page, idx, "amazon_captcha")
                return "CAPTCHA", "CAPTCHA"
            
            data = await page.evaluate('''() => {
                let sellingPrice = '';
                let mrp = '';
                
                const sellingEl = document.querySelector('.a-price:not([data-a-strike="true"]) .a-offscreen');
                if (sellingEl) sellingPrice = sellingEl.textContent.trim();
                
                const mrpEl = document.querySelector('.a-text-price .a-offscreen');
                if (mrpEl) mrp = mrpEl.textContent.trim();
                
                if (!mrp && sellingPrice) mrp = sellingPrice;
                
                return { mrp, sellingPrice };
            }''')
            
            return data.get('mrp') or "N/A", data.get('sellingPrice') or "N/A"
            
        except Exception as e:
            print(f"  ✗ Direct Amazon error: {e}")
            return "Error", "Error"
        finally:
            await context.close()

    async def _scrape_ebazaar(self, url: str, idx: int) -> tuple:
        """Scrape eBazaar using Playwright"""
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

    async def _save_debug(self, page, idx: int, source: str):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            await page.screenshot(path=f"{self.debug_dir}/{source}_{idx}_{timestamp}.png", full_page=True)
            print(f"  📸 Debug saved: {source}_{idx}_{timestamp}.png")
        except Exception as e:
            print(f"  Failed to save debug: {e}")


def main():
    print("=" * 60)
    print("Price Scraper Started")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    scraper = UnifiedScraper(debug_mode=True)
    
    try:
        df_output = asyncio.run(scraper.run('price/input_links.csv'))
        df_output.to_csv("price/data.csv", index=False)
        
        print("\n" + "=" * 60)
        print(f"✓ Saved {len(df_output)} products to price/data.csv")
        
        amazon_ok = len(df_output[~df_output['amazon_selling_price'].isin(['N/A', 'Error', 'CAPTCHA', 'Blocked', ''])])
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
