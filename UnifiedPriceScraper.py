import asyncio
import random
import os
from dataclasses import dataclass, asdict
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
    def __init__(self):
        self.browser = None
        self.page = None

    async def run(self, input_csv: str) -> pd.DataFrame:
        df_input = pd.read_csv(input_csv)
        products = []
        
        async with async_playwright() as p:
            await self._setup_browser(p)
            
            for idx, row in df_input.iterrows():
                product = ProductComparison(
                    model_name=row['model_name'],
                    amazon_link=row['amazon_link'],
                    ebazaar_link=row['ebazaar_link']
                )
                
                if pd.notna(row['amazon_link']) and row['amazon_link'].strip():
                    mrp, selling = await self._scrape_amazon(row['amazon_link'])
                    product.amazon_mrp = mrp
                    product.amazon_selling_price = selling
                
                await self.page.wait_for_timeout(random.randint(2000, 3000))
                
                if pd.notna(row['ebazaar_link']) and row['ebazaar_link'].strip():
                    mrp, selling = await self._scrape_ebazaar(row['ebazaar_link'])
                    product.ebazaar_mrp = selling
                    product.ebazaar_selling_price = mrp
                
                products.append(product)
                print(f"Model {idx + 1} done")
                
                if idx < len(df_input) - 1:
                    await self.page.wait_for_timeout(random.randint(3000, 5000))
            
            await self.browser.close()
        
        return pd.DataFrame([asdict(p) for p in products])

    async def _setup_browser(self, playwright):
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
        )
        self.page = await context.new_page()

    async def _scrape_amazon(self, url: str) -> tuple:
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(3000)
            
            data = await self.page.evaluate('''() => {
                let sellingPrice = '';
                let mrp = '';
                
                const priceEl = document.querySelector('.a-price .a-offscreen');
                if (priceEl) sellingPrice = priceEl.textContent.trim();
                
                const mrpEl = document.querySelector('.a-text-price .a-offscreen, .basisPrice .a-offscreen');
                if (mrpEl) mrp = mrpEl.textContent.trim();
                
                return { mrp, sellingPrice };
            }''')
            
            return data.get('mrp') or "N/A", data.get('sellingPrice') or "N/A"
        except:
            return "Error", "Error"

    async def _scrape_ebazaar(self, url: str) -> tuple:
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(3000)
            
            data = await self.page.evaluate('''() => {
                let sellingPrice = '';
                let mrp = '';
                
                const priceContainer = document.querySelector('.product-info-price, .price-box, .product-info-main');
                
                if (priceContainer) {
                    const allText = priceContainer.innerText;
                    const priceMatches = allText.match(/\\$[\\d,]+/g);
                    
                    if (priceMatches && priceMatches.length >= 2) {
                        sellingPrice = priceMatches[0];
                        mrp = priceMatches[1];
                    } else if (priceMatches && priceMatches.length === 1) {
                        sellingPrice = priceMatches[0];
                    }
                }
                
                if (!sellingPrice) {
                    const sellingEl = document.querySelector('[data-price-type="finalPrice"]');
                    if (sellingEl) {
                        const amount = sellingEl.getAttribute('data-price-amount');
                        if (amount) sellingPrice = '$' + parseFloat(amount).toFixed(0);
                    }
                }
                
                if (!mrp) {
                    const mrpEl = document.querySelector('[data-price-type="oldPrice"]');
                    if (mrpEl) {
                        const amount = mrpEl.getAttribute('data-price-amount');
                        if (amount) mrp = '$' + parseFloat(amount).toFixed(0);
                    }
                }
                
                return { mrp, sellingPrice };
            }''')
            
            return data.get('mrp') or "N/A", data.get('sellingPrice') or "N/A"
        except:
            return "Error", "Error"


def main():
    # Run scraper
    scraper = UnifiedScraper()
    df_output = asyncio.run(scraper.run('price/input_links.csv'))
    
    # Save output
    df_output.to_csv("price/data.csv", index=False)
    print(f"Done. Saved {len(df_output)} products to price/data.csv")


if __name__ == "__main__":
    main()
