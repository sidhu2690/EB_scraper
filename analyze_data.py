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
    
    # Clean price column
    if 'price' in df.columns:
        df['price'] = df['price'].astype(str).str.replace('$', '', regex=False)
        df['price'] = df['price'].str.replace(',', '', regex=False)
        df['price'] = df['price'].str.strip()
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    return df

def create_focused_comparison(weekly_df, daily_df):
    """Create comparison focused on rank changes, new products, and price changes"""
    weekly_df = clean_dataframe(weekly_df)
    daily_df = clean_dataframe(daily_df)
    
    output = ""
    
    # Create lookup dictionaries using ASIN (more reliable) or name
    if 'asin' in weekly_df.columns and 'asin' in daily_df.columns:
        key_col = 'asin'
    else:
        key_col = 'name'
    
    weekly_products = {}
    for _, row in weekly_df.iterrows():
        key = str(row.get(key_col, ''))
        weekly_products[key] = {
            'rank': row.get('rank'),
            'price': row.get('price'),
            'name': row.get('name', '')[:70]
        }
    
    daily_products = {}
    for _, row in daily_df.iterrows():
        key = str(row.get(key_col, ''))
        daily_products[key] = {
            'rank': row.get('rank'),
            'price': row.get('price'),
            'name': row.get('name', '')[:70]
        }
    
    # Find product sets
    common = set(weekly_products.keys()) & set(daily_products.keys())
    new_products = set(daily_products.keys()) - set(weekly_products.keys())
    removed_products = set(weekly_products.keys()) - set(daily_products.keys())
    
    # ===== SECTION 1: RANK CHANGES =====
    output += "="*60 + "\n"
    output += "1. RANK CHANGES (Products in both weeks)\n"
    output += "="*60 + "\n\n"
    
    rank_changes = []
    for key in common:
        old_rank = weekly_products[key]['rank']
        new_rank = daily_products[key]['rank']
        name = daily_products[key]['name']
        if pd.notna(old_rank) and pd.notna(new_rank):
            change = int(old_rank) - int(new_rank)  # Positive = moved UP
            rank_changes.append((name, int(old_rank), int(new_rank), change))
    
    # Products that moved UP
    moved_up = [(n, o, nw, c) for n, o, nw, c in rank_changes if c > 0]
    moved_up.sort(key=lambda x: x[3], reverse=True)
    
    output += "📈 MOVED UP IN RANK:\n"
    output += f"{'Product':<50} {'Last Week':<12} {'Now':<12} {'Change'}\n"
    output += "-"*85 + "\n"
    if moved_up:
        for name, old, new, change in moved_up:
            output += f"{name:<50} #{old:<11} #{new:<11} ⬆️ +{change}\n"
    else:
        output += "None\n"
    
    # Products that moved DOWN
    moved_down = [(n, o, nw, c) for n, o, nw, c in rank_changes if c < 0]
    moved_down.sort(key=lambda x: x[3])
    
    output += "\n📉 MOVED DOWN IN RANK:\n"
    output += f"{'Product':<50} {'Last Week':<12} {'Now':<12} {'Change'}\n"
    output += "-"*85 + "\n"
    if moved_down:
        for name, old, new, change in moved_down:
            output += f"{name:<50} #{old:<11} #{new:<11} ⬇️ {change}\n"
    else:
        output += "None\n"
    
    # Products with NO change
    no_change = [(n, o, nw, c) for n, o, nw, c in rank_changes if c == 0]
    output += f"\n➡️ NO RANK CHANGE: {len(no_change)} products\n"
    
    # ===== SECTION 2: NEW PRODUCTS =====
    output += "\n" + "="*60 + "\n"
    output += "2. NEW PRODUCTS (Not in last week's list)\n"
    output += "="*60 + "\n\n"
    
    if new_products:
        output += f"{'Rank':<8} {'Price':<12} {'Product'}\n"
        output += "-"*80 + "\n"
        new_list = []
        for key in new_products:
            new_list.append((
                daily_products[key]['rank'],
                daily_products[key]['price'],
                daily_products[key]['name']
            ))
        new_list.sort(key=lambda x: x[0] if pd.notna(x[0]) else 999)
        for rank, price, name in new_list:
            price_str = f"${price:.2f}" if pd.notna(price) else "N/A"
            output += f"#{rank:<7} {price_str:<12} {name}\n"
    else:
        output += "No new products this week\n"
    
    # ===== SECTION 3: REMOVED PRODUCTS =====
    output += "\n" + "="*60 + "\n"
    output += "3. REMOVED PRODUCTS (Were in last week, now gone)\n"
    output += "="*60 + "\n\n"
    
    if removed_products:
        output += f"{'Was Rank':<10} {'Price':<12} {'Product'}\n"
        output += "-"*80 + "\n"
        removed_list = []
        for key in removed_products:
            removed_list.append((
                weekly_products[key]['rank'],
                weekly_products[key]['price'],
                weekly_products[key]['name']
            ))
        removed_list.sort(key=lambda x: x[0] if pd.notna(x[0]) else 999)
        for rank, price, name in removed_list:
            price_str = f"${price:.2f}" if pd.notna(price) else "N/A"
            output += f"#{rank:<9} {price_str:<12} {name}\n"
    else:
        output += "No products removed this week\n"
    
    # ===== SECTION 4: PRICE CHANGES =====
    output += "\n" + "="*60 + "\n"
    output += "4. PRICE CHANGES (Same product, different price)\n"
    output += "="*60 + "\n\n"
    
    price_changes = []
    for key in common:
        old_price = weekly_products[key]['price']
        new_price = daily_products[key]['price']
        name = daily_products[key]['name']
        new_rank = daily_products[key]['rank']
        if pd.notna(old_price) and pd.notna(new_price) and old_price != new_price:
            change = new_price - old_price
            pct = (change / old_price * 100) if old_price != 0 else 0
            price_changes.append((name, new_rank, old_price, new_price, change, pct))
    
    # Price DECREASED
    price_down = [(n, r, o, nw, c, p) for n, r, o, nw, c, p in price_changes if c < 0]
    price_down.sort(key=lambda x: x[4])
    
    output += "💰 PRICE DECREASED:\n"
    output += f"{'Product':<40} {'Rank':<6} {'Was':<10} {'Now':<10} {'Change'}\n"
    output += "-"*85 + "\n"
    if price_down:
        for name, rank, old, new, change, pct in price_down:
            output += f"{name[:40]:<40} #{rank:<5} ${old:<9.2f} ${new:<9.2f} -${abs(change):.2f} ({pct:.1f}%)\n"
    else:
        output += "None\n"
    
    # Price INCREASED
    price_up = [(n, r, o, nw, c, p) for n, r, o, nw, c, p in price_changes if c > 0]
    price_up.sort(key=lambda x: x[4], reverse=True)
    
    output += "\n💸 PRICE INCREASED:\n"
    output += f"{'Product':<40} {'Rank':<6} {'Was':<10} {'Now':<10} {'Change'}\n"
    output += "-"*85 + "\n"
    if price_up:
        for name, rank, old, new, change, pct in price_up:
            output += f"{name[:40]:<40} #{rank:<5} ${old:<9.2f} ${new:<9.2f} +${change:.2f} (+{pct:.1f}%)\n"
    else:
        output += "None\n"
    
    # No price change count
    no_price_change = len(common) - len(price_changes)
    output += f"\n➡️ NO PRICE CHANGE: {no_price_change} products\n"
    
    # ===== SUMMARY STATS =====
    output += "\n" + "="*60 + "\n"
    output += "SUMMARY\n"
    output += "="*60 + "\n"
    output += f"Total products last week: {len(weekly_products)}\n"
    output += f"Total products this week: {len(daily_products)}\n"
    output += f"Products in both weeks: {len(common)}\n"
    output += f"New products: {len(new_products)}\n"
    output += f"Removed products: {len(removed_products)}\n"
    output += f"Products moved up: {len(moved_up)}\n"
    output += f"Products moved down: {len(moved_down)}\n"
    output += f"Price decreased: {len(price_down)}\n"
    output += f"Price increased: {len(price_up)}\n"
    
    return output

def analyze_data():
    """Analyze weekly vs daily data using Groq API"""
    
    daily_df = load_csv_data("data/data.csv")
    weekly_df = load_csv_data("data/weekly.csv")
    
    if daily_df is None or weekly_df is None:
        print("Need both weekly.csv and data.csv for comparison")
        return None
    
    print("📊 Creating focused comparison (ranks & prices only)...")
    
    # Create the comparison data
    comparison_data = create_focused_comparison(weekly_df, daily_df)
    
    prompt = """Analyze this Amazon Laptop Bestseller comparison data. Focus ONLY on:

1. **Ranking Changes** - What moved up? What moved down? Any big jumps?
2. **New Products** - What's new this week? At what rank did they enter?
3. **Removed Products** - What dropped off? Were they previously high-ranked?
4. **Price Changes** - Which products got cheaper? Which got more expensive?

DO NOT include:
- Recommendations
- Brand analysis
- General market commentary
- Average price calculations

Just summarize the CHANGES in a clear, concise format.

DATA:
"""
    prompt += comparison_data
    
    prompt += """

Provide a brief, focused summary of:
1. Most notable rank changes (biggest movers)
2. Interesting new entries
3. Significant price drops or increases
4. Any patterns you notice in the changes

Keep it short and factual. Use bullet points."""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    try:
        print("🤖 Sending to Groq API...")
        
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.5
        )
        
        analysis = completion.choices[0].message.content
        print("✅ Analysis completed")
        return comparison_data + "\n\n" + "="*60 + "\nAI ANALYSIS\n" + "="*60 + "\n\n" + analysis
        
    except Exception as e:
        print(f"❌ Error: {e}")
        # Return just the raw comparison if API fails
        return comparison_data + "\n\n[AI Analysis unavailable - raw data above]"

def save_analysis(analysis):
    """Save analysis to summary.txt"""
    if analysis:
        os.makedirs("data", exist_ok=True)
        with open("data/summary.txt", "w") as f:
            f.write("AMAZON LAPTOP BESTSELLERS - WEEKLY CHANGES REPORT\n")
            f.write("="*60 + "\n\n")
            f.write(analysis)
        print("💾 Saved to data/summary.txt")
        return True
    return False

def main():
    print("🔍 Starting weekly comparison...")
    analysis = analyze_data()
    if analysis:
        save_analysis(analysis)
        print("✅ Done")
    else:
        print("❌ Failed")

if __name__ == "__main__":
    main()
