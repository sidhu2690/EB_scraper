import os
import pandas as pd
from groq import Groq

def load_csv_data(filepath):
    """Load CSV file and return dataframe"""
    try:
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            return df
        else:
            return None
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def clean_dataframe(df):
    """Clean and standardize the dataframe"""
    df = df.copy()
    
    # Rename columns to standard names
    column_mapping = {
        'k': 'rank',
        'reviews_count': 'reviews'
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Clean price column - remove $, commas, and convert to float
    if 'price' in df.columns:
        df['price'] = df['price'].astype(str).str.replace('$', '', regex=False)
        df['price'] = df['price'].str.replace(',', '', regex=False)
        df['price'] = df['price'].str.strip()
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Clean rating column
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    
    # Clean reviews column
    if 'reviews' in df.columns:
        df['reviews'] = df['reviews'].astype(str).str.replace(',', '', regex=False)
        df['reviews'] = pd.to_numeric(df['reviews'], errors='coerce')
    
    return df

def format_product_list(df, label):
    """Format complete product list with rank, name, and price"""
    df = clean_dataframe(df)
    
    output = f"\n{'='*60}\n{label}\n{'='*60}\n"
    output += f"Total Products: {len(df)}\n\n"
    output += f"{'Rank':<6} {'Price':<12} {'Product Name'}\n"
    output += f"{'-'*6} {'-'*12} {'-'*50}\n"
    
    for _, row in df.iterrows():
        rank = row.get('rank', 'N/A')
        price = row.get('price', None)
        price_str = f"${price:.2f}" if pd.notna(price) else "N/A"
        name = row.get('name', 'N/A')
        # Truncate name if too long to save tokens but keep it meaningful
        if len(str(name)) > 80:
            name = str(name)[:77] + "..."
        output += f"{rank:<6} {price_str:<12} {name}\n"
    
    # Add price statistics
    if 'price' in df.columns:
        price_data = df['price'].dropna()
        if len(price_data) > 0:
            output += f"\n--- Price Statistics ---\n"
            output += f"Average: ${price_data.mean():.2f}\n"
            output += f"Min: ${price_data.min():.2f}\n"
            output += f"Max: ${price_data.max():.2f}\n"
            output += f"Median: ${price_data.median():.2f}\n"
    
    # Add rating statistics
    if 'rating' in df.columns:
        rating_data = df['rating'].dropna()
        if len(rating_data) > 0:
            output += f"\n--- Rating Statistics ---\n"
            output += f"Average Rating: {rating_data.mean():.2f}\n"
            output += f"Highest: {rating_data.max():.2f}\n"
            output += f"Lowest: {rating_data.min():.2f}\n"
    
    return output

def create_comparison_table(weekly_df, daily_df):
    """Create a side-by-side comparison of products that changed"""
    weekly_df = clean_dataframe(weekly_df)
    daily_df = clean_dataframe(daily_df)
    
    comparison = "\n" + "="*60 + "\n"
    comparison += "SIDE-BY-SIDE COMPARISON\n"
    comparison += "="*60 + "\n\n"
    
    # Create lookup by product name (or ASIN if available)
    weekly_products = {}
    for _, row in weekly_df.iterrows():
        name = str(row.get('name', ''))[:50]  # Use first 50 chars as key
        weekly_products[name] = {
            'rank': row.get('rank'),
            'price': row.get('price'),
            'rating': row.get('rating')
        }
    
    daily_products = {}
    for _, row in daily_df.iterrows():
        name = str(row.get('name', ''))[:50]
        daily_products[name] = {
            'rank': row.get('rank'),
            'price': row.get('price'),
            'rating': row.get('rating')
        }
    
    # Find products in both weeks
    common_products = set(weekly_products.keys()) & set(daily_products.keys())
    new_products = set(daily_products.keys()) - set(weekly_products.keys())
    removed_products = set(weekly_products.keys()) - set(daily_products.keys())
    
    comparison += f"Products in both weeks: {len(common_products)}\n"
    comparison += f"New products this week: {len(new_products)}\n"
    comparison += f"Products no longer in list: {len(removed_products)}\n\n"
    
    # Show price changes for common products
    comparison += "--- Price Changes (Common Products) ---\n"
    comparison += f"{'Product':<40} {'Last Week':<12} {'This Week':<12} {'Change'}\n"
    comparison += "-"*80 + "\n"
    
    price_changes = []
    for name in common_products:
        old_price = weekly_products[name]['price']
        new_price = daily_products[name]['price']
        if pd.notna(old_price) and pd.notna(new_price):
            change = new_price - old_price
            pct_change = (change / old_price * 100) if old_price != 0 else 0
            price_changes.append((name, old_price, new_price, change, pct_change))
    
    # Sort by absolute change
    price_changes.sort(key=lambda x: abs(x[3]), reverse=True)
    
    for name, old_price, new_price, change, pct_change in price_changes[:20]:  # Top 20 changes
        change_str = f"${change:+.2f} ({pct_change:+.1f}%)"
        comparison += f"{name[:40]:<40} ${old_price:<11.2f} ${new_price:<11.2f} {change_str}\n"
    
    # Show new products
    if new_products:
        comparison += f"\n--- New Products This Week ---\n"
        for name in list(new_products)[:15]:  # Show up to 15
            price = daily_products[name]['price']
            price_str = f"${price:.2f}" if pd.notna(price) else "N/A"
            comparison += f"  • {name[:60]} - {price_str}\n"
    
    # Show removed products
    if removed_products:
        comparison += f"\n--- Products No Longer in Top List ---\n"
        for name in list(removed_products)[:15]:  # Show up to 15
            price = weekly_products[name]['price']
            price_str = f"${price:.2f}" if pd.notna(price) else "N/A"
            comparison += f"  • {name[:60]} - {price_str}\n"
    
    return comparison

def analyze_data():
    """Analyze daily and weekly CSV data using Groq API"""
    
    # Load both CSV files
    daily_df = load_csv_data("data/data.csv")
    weekly_df = load_csv_data("data/weekly.csv")
    
    if daily_df is None and weekly_df is None:
        print("No data files found to analyze")
        return None
    
    if daily_df is None or weekly_df is None:
        print("Need both daily and weekly data for comparison")
        return None
    
    print("📊 Formatting complete data for analysis...")
    
    # Create complete product lists
    weekly_list = format_product_list(weekly_df, "LAST WEEK'S COMPLETE DATA")
    daily_list = format_product_list(daily_df, "TODAY'S COMPLETE DATA")
    
    # Create comparison table
    comparison = create_comparison_table(weekly_df, daily_df)
    
    # Build the prompt
    prompt = """You are a data analyst specializing in e-commerce trends. I'm providing you with COMPLETE Amazon Best Sellers Laptop data from two time periods.

IMPORTANT: 
- "Last Week's Data" is from 7 days ago
- "Today's Data" is the most recent scrape
- Analyze ALL products, not just the top 10

"""
    prompt += weekly_list
    prompt += "\n"
    prompt += daily_list
    prompt += "\n"
    prompt += comparison
    
    prompt += """

Based on the COMPLETE data above, provide a thorough analysis:

1. **Ranking Changes**
   - Which products moved up significantly in rank?
   - Which products dropped in rank?
   - New entries to the bestseller list
   - Products that fell off the list

2. **Price Analysis**
   - Products with the biggest price drops (deals/sales?)
   - Products with price increases
   - Overall price trend direction
   - Best value products (low price + good rank)

3. **Market Trends**
   - Which brands are gaining/losing ground?
   - Price range distribution changes
   - Are budget or premium laptops trending?

4. **Notable Observations**
   - Any surprising changes?
   - Seasonal patterns if apparent
   - Product categories doing well (Chromebooks, MacBooks, Gaming, etc.)

5. **Recommendations**
   - Best deals this week (price drops)
   - Products to watch
   - Buying recommendations

Be specific with product names and numbers. Reference actual data from the lists.
Format for an email report with clear sections."""

    # Initialize Groq client
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    try:
        print("🤖 Sending complete data to Groq API...")
        print(f"📝 Total prompt length: ~{len(prompt)} characters")
        
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=4000,
            temperature=0.7
        )
        
        analysis = completion.choices[0].message.content
        print("✅ Analysis completed successfully")
        return analysis
        
    except Exception as e:
        print(f"❌ Error calling Groq API: {e}")
        # If token limit exceeded, try with reduced data
        if "token" in str(e).lower() or "limit" in str(e).lower():
            print("⚠️ Token limit hit, trying with reduced data...")
            return analyze_data_reduced(weekly_df, daily_df)
        return None

def analyze_data_reduced(weekly_df, daily_df):
    """Fallback: Analyze with top 30 products if full data exceeds limits"""
    print("📊 Using reduced dataset (top 30 products)...")
    
    weekly_df = clean_dataframe(weekly_df).head(30)
    daily_df = clean_dataframe(daily_df).head(30)
    
    weekly_list = format_product_list(weekly_df, "LAST WEEK'S TOP 30")
    daily_list = format_product_list(daily_df, "TODAY'S TOP 30")
    comparison = create_comparison_table(weekly_df, daily_df)
    
    prompt = """Analyze this Amazon Laptop Bestseller data (Top 30 products):

"""
    prompt += weekly_list + "\n" + daily_list + "\n" + comparison
    prompt += """

Provide analysis of:
1. Ranking changes
2. Price changes (deals, increases)
3. Brand trends
4. Best value products
5. Recommendations

Be specific with product names and prices."""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0.7
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"❌ Reduced analysis also failed: {e}")
        return None

def save_analysis(analysis):
    """Save analysis to summary.txt file"""
    if analysis:
        os.makedirs("data", exist_ok=True)
        with open("data/summary.txt", "w") as f:
            f.write("=" * 60 + "\n")
            f.write("AMAZON LAPTOP BESTSELLERS - WEEKLY ANALYSIS REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write("Comparing: Last Week's Data vs Today's Data\n")
            f.write("-" * 60 + "\n\n")
            f.write(analysis)
        print("💾 Analysis saved to data/summary.txt")
        return True
    return False

def main():
    print("🔍 Starting weekly data analysis...")
    print("📊 Comparing COMPLETE datasets: last week vs today...")
    
    analysis = analyze_data()
    
    if analysis:
        save_analysis(analysis)
        print("✅ Weekly analysis complete")
    else:
        print("❌ Analysis failed - need both weekly.csv and data.csv")

if __name__ == "__main__":
    main()
