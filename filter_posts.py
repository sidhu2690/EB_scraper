import pandas as pd
import joblib
import os
from sentence_transformers import SentenceTransformer

# ===========================
#  LOAD MODEL + CLASSIFIER
# ===========================

# Ensure embedder directory exists (it is extracted by workflow)
if not os.path.exists("miniLM_embedder"):
    raise FileNotFoundError("miniLM_embedder folder not found. Did you unzip filter_model.zip?")

# Load the embedding model
embedder = SentenceTransformer("miniLM_embedder")

# Load the classifier
clf = joblib.load("usefulness_classifier.pkl")

print("Model + Classifier loaded successfully.")


# ===========================
#  LOAD POSTS.CSV
# ===========================

if not os.path.exists("posts.csv"):
    raise FileNotFoundError("posts.csv not found. Scraper must run first.")

df = pd.read_csv("posts.csv")

# If no rows, exit cleanly
if df.empty:
    print("posts.csv is empty. Nothing to filter.")
    open("filtered_posts.csv", "w").close()
    exit(0)

print(f"Loaded {len(df)} posts.")


# ===========================
#  EMBED & CLASSIFY
# ===========================

titles = df["Title"].astype(str).tolist()

print("Encoding titles...")
embeddings = embedder.encode(titles, batch_size=32, show_progress_bar=False)

print("Predicting usefulness...")
preds = clf.predict(embeddings)

df["Useful"] = preds


# ===========================
#  FILTER ONLY USEFUL POSTS
# ===========================

filtered_df = df[df["Useful"] == 1].copy()

print(f"Filtered: {len(filtered_df)} useful posts out of {len(df)} total.")


# ===========================
#  SAVE OUTPUT
# ===========================

filtered_df.to_csv("filtered_posts.csv", index=False)
print("Saved filtered_posts.csv successfully!")
