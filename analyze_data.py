import os
import pandas as pd
from groq import Groq

def load_csv_data(filepath):
    """Load CSV file and return as string summary"""
    try:
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            return df.to_string()
        else:
            return None
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def analyze_data():
    """Analyze daily and weekly CSV data using Groq API"""
    
    # Load both CSV files
    daily_data = load_csv_data("data/data.csv")
    weekly_data = load_csv_data("data/weekly.csv")
    
    if not daily_data and not weekly_data:
        print("No data files found to analyze")
        return None
    
    # Build the prompt
    prompt = """You are a data analyst. Analyze the following Amazon Best Sellers Laptop data and provide insights.

"""
    
    if daily_data:
        prompt += f"""## Daily Data (data.csv):
{daily_data}

"""
    
    if weekly_data:
        prompt += f"""## Weekly Data (weekly.csv):
{weekly_data}

"""
    
    prompt += """Please provide:
1. **Top 5 Best Selling Laptops** - List the top performers with their prices and ratings
2. **Price Analysis** - Average price, price range, and any notable pricing trends
3. **Rating Analysis** - Average rating, best rated laptops
4. **Brand Analysis** - Which brands appear most frequently in the bestsellers
5. **Comparison** (if both daily and weekly data available) - Any notable changes or trends between daily and weekly data
6. **Key Insights** - Any interesting patterns or recommendations

Format your response in a clear, readable format suitable for an email report."""

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
            max_tokens=2000,
            temperature=0.7
        )
        
        analysis = completion.choices[0].message.content
        print("Analysis completed successfully")
        return analysis
        
    except Exception as e:
        print(f"Error calling Groq API: {e}")
        return None

def save_analysis(analysis):
    """Save analysis to a text file"""
    if analysis:
        os.makedirs("data", exist_ok=True)
        with open("data/analysis_report.txt", "w") as f:
            f.write("=" * 60 + "\n")
            f.write("AMAZON LAPTOP BESTSELLERS ANALYSIS REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(analysis)
        print("Analysis saved to data/analysis_report.txt")
        return True
    return False

def main():
    print("🔍 Starting data analysis...")
    analysis = analyze_data()
    
    if analysis:
        save_analysis(analysis)
        print("✅ Analysis complete")
    else:
        print("❌ Analysis failed")

if __name__ == "__main__":
    main()
