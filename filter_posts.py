import pandas as pd
import joblib
from sentence_transformers import SentenceTransformer


class PostFilter:

    def __init__(self, input_csv="posts.csv", output_csv="filtered_posts.csv", classifier_file="usefulness_classifier.pkl"):
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.classifier_file = classifier_file

        # Load MiniLM model
        print("⚙️ Loading MiniLM sentence transformer...")
        self.embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        # Load classifier
        print("⚙️ Loading usefulness classifier...")
        self.clf = joblib.load(self.classifier_file)

    # ====================== LOAD POSTS ======================

    def load_posts(self):
        print(f"\n📥 Loading posts from {self.input_csv}...")
        df = pd.read_csv(self.input_csv)

        if df.empty:
            print("⚠️ posts.csv is empty — nothing to filter.")
            return pd.DataFrame(columns=["Title", "Link", "Unique_ID"])

        print(f"📊 Loaded {len(df)} posts.")
        return df

    # ====================== EMBED & PREDICT ======================

    def predict_usefulness(self, df):
        print("\n🧠 Creating embeddings...")
        titles = df["Title"].astype(str).tolist()
        embeddings = self.embedder.encode(titles, batch_size=32, show_progress_bar=False)

        print("🔍 Predicting usefulness...")
        preds = self.clf.predict(embeddings)
        df["Useful"] = preds
        return df

    # ====================== FILTER & KEEP UNIQUE ======================

    def filter_and_deduplicate(self, df):
        print("\n🔎 Filtering only useful posts...")
        filtered = df[df["Useful"] == 1][["Title", "Link", "Unique_ID"]]

        print(f"📉 Before removing duplicates: {len(filtered)} posts")

        # Remove duplicates based on Unique_ID (like your scraper logic)
        filtered = filtered.drop_duplicates(subset="Unique_ID")

        print(f"📈 After removing duplicates: {len(filtered)} posts remain")
        return filtered

    # ====================== SAVE ======================

    def save_filtered(self, filtered_df):
        filtered_df.to_csv(self.output_csv, index=False)
        print(f"\n💾 Saved filtered posts → {self.output_csv}")

    # ====================== RUN FULL PIPELINE ======================

    def run(self):
        print("\n🚀 Starting ML Filtering Pipeline...")

        df = self.load_posts()

        if df.empty:
            self.save_filtered(df)
            return

        df = self.predict_usefulness(df)
        filtered = self.filter_and_deduplicate(df)
        self.save_filtered(filtered)

        print("\n✅ Filtering Completed Successfully!\n")


# ====================== MAIN EXECUTION ======================

if __name__ == "__main__":
    PostFilter().run()
