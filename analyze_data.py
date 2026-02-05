import os
import pandas as pd
from groq import Groq

def load_csv_data(filepath):
    """Load CSV file and return as string summary"""
    try:
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            return df, df.to_string()
        else:
            return None, None
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None, None

def clean_numeric_columns(df):
    """Clean and convert price/rating columns to numeric"""
    df = df.copy()
    
    # Rename columns to expected names if they exist
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

def summarize_dataframe(df, label):
    """Create a concise summary of the dataframe for API consumption"""
    # Clean numeric columns first
    df = clean_numeric_columns(df)
    
    summary = f"\n{label}:\n"
    summary += f"Total Products: {len(df)}\n\n"
    
    # Get top 10 products with key info
    summary += "Top 10 Products:\n"
    for idx, row in df.head(10).iterrows():
        summary += f"{idx+1}. {row.get('name', 'N/A')}\n"
        summary += f"   Rank: {row.get('rank', 'N/A')} | "
        price_val = row.get('price', None)
        price_str = f"${price_val:.2f}" if pd.notna(price_val) else "N/A"
        summary += f"Price: {price_str} | "
        rating_val = row.get('rating', 'N/A')
        reviews_val = row.get('reviews', 'N/A')
        summary += f"Rating: {rating_val} ({reviews_val} reviews)\n"
    
    # Add statistical summary
    summary += f"\nPrice Statistics:\n"
    if 'price' in df.columns:
        price_data = df['price'].dropna()
        if len(price_data) > 0:
            summary += f"  Average: ${price_data.mean():.2f}\n"
            summary += f"  Min: ${price_data.min():.2f}\n"
            summary += f"  Max: ${price_data.max():.2f}\n"
        else:
            summary += "  No valid price data available\n"
    
    summary += f"\nRating Statistics:\n"
    if 'rating' in df.columns:
        rating_data = df['rating'].dropna()
        if len(rating_data) > 0:
            summary += f"  Average Rating: {rating_data.mean():.2f}\n"
            summary += f"  Highest Rated: {rating_data.max():.2f}\n"
        else:
            summary += "  No valid rating data available\n"
    
    # Brand distribution (top 5)
    if 'name' in df.columns:
        summary += f"\nTop 5 Brands (by product count):\n"
        brands = df['name'].str.split().str[0].value_counts().head(5)
        for brand, count in brands.items():
            summary += f"  {brand}: {count} products\n"
    
    return summary

def analyze_data():
    """Analyze daily and weekly CSV data using Groq API"""
    
    # Load both CSV files
    daily_df, _ = load_csv_data("data/data.csv")
    weekly_df, _ = load_csv_data("data/weekly.csv")
    
    if daily_df is None and weekly_df is None:
        print("No data files found to analyze")
        return None
    
    if daily_df is None or weekly_df is None:
        print("Need both daily and weekly data for comparison")
        return None
    
    # Create summarized versions to reduce token usage
    print("📊 Creating data summaries to fit API limits...")
    weekly_summary = summarize_dataframe(weekly_df, "LAST WEEK'S DATA")
    daily_summary = summarize_dataframe(daily_df, "TODAY'S DATA")
    
    # Build the prompt with summaries instead of full data
    prompt = """You are a data analyst specializing in e-commerce trends. Analyze the following Amazon Best Sellers Laptop data summaries.

IMPORTANT: 
- "Last Week's Data" is from 7 days ago
- "Today's Data" is the most recent data

Compare these two datasets to identify trends and changes over the past week.
"""
    prompt += weekly_summary
    prompt += "\n" + "="*60 + "\n"
    prompt += daily_summary
    
    prompt += """

Based on the above summaries, please provide a comprehensive analysis including:

1. **Week-over-Week Comparison**
   - Notable changes in the top 10 rankings
   - Any significant movements you can identify
   - New products that appeared in top 10

2. **Price Changes**
   - Compare average, min, and max prices
   - Note any significant price shifts
   - Price trend direction (increasing/decreasing)

3. **Top Performers**
   - Products that maintained top positions
   - Best rated products
   - Value-for-money options

4. **Brand Analysis**
   - Which brands are dominating the top 10
   - Any brand presence changes
   - Brand diversity in the market

5. **Rating & Review Trends**
   - Average rating comparison
   - Review count trends
   - Quality indicators

6. **Key Insights & Recommendations**
   - Overall market trends
   - Notable patterns or shifts
   - Buying recommendations based on the data

Format your response in a clear, professional format suitable for an email report.
Use bullet points and clear headings for readability."""

    # Initialize Groq client
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    try:
        print("🤖 Sending request to Groq API...")
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=3000,
            temperature=0.7
        )
        
        analysis = completion.choices[0].message.content
        print("✅ Analysis completed successfully")
        return analysis
        
    except Exception as e:
        print(f"❌ Error calling Groq API: {e}")
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
    print("📊 Comparing last week's data with today's data...")
    
    analysis = analyze_data()
    
    if analysis:
        save_analysis(analysis)
        print("✅ Weekly analysis complete")
    else:
        print("❌ Analysis failed - need both weekly.csv and data.csv")

if __name__ == "__main__":
    main()
