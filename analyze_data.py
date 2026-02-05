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

def analyze_data():
    """Analyze daily and weekly CSV data using Groq API"""
    
    # Load both CSV files
    daily_df, daily_data = load_csv_data("data/data.csv")
    weekly_df, weekly_data = load_csv_data("data/weekly.csv")
    
    if daily_data is None and weekly_data is None:
        print("No data files found to analyze")
        return None
    
    if daily_data is None or weekly_data is None:
        print("Need both daily and weekly data for comparison")
        return None
    
    # Build the prompt
    prompt = """You are a data analyst specializing in e-commerce trends. Analyze the following Amazon Best Sellers Laptop data.

IMPORTANT: 
- "Weekly Data" is from LAST WEEK (7 days ago)
- "Daily Data" is from TODAY (most recent)

Compare these two datasets to identify trends and changes over the past week.

## Weekly Data (from last week - weekly.csv):
"""
    prompt += weekly_data
    
    prompt += """

## Daily Data (from today - data.csv):
"""
    prompt += daily_data
    
    prompt += """

Please provide a comprehensive analysis including:

1. **Week-over-Week Comparison**
   - New laptops that entered the bestseller list this week
   - Laptops that dropped off the list
   - Significant rank changes (moved up or down significantly)

2. **Price Changes**
   - Any notable price increases or decreases
   - Average price comparison (last week vs this week)

3. **Top Performers**
   - Top 5 laptops that maintained or improved their position
   - Best rated laptops

4. **Brand Analysis**
   - Which brands are dominating
   - Any brand gaining or losing presence

5. **Key Insights & Trends**
   - Overall market trends
   - Notable patterns
   - Any recommendations for buyers

Format your response in a clear, professional format suitable for an email report.
Use bullet points and clear headings for readability."""

    # Initialize Groq client
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    try:
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
