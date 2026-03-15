# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

# =========================
# PATHS
# =========================
BASE_DIR = "/content/drive/MyDrive/Colab Notebooks/intelligence_ai_marketing"

ADS_PATH = f"{BASE_DIR}/data/collected_ads_enriched.csv"
NLP_PATH = f"{BASE_DIR}/nlp/image_text_analysis.csv"
OUTPUT_PATH = f"{BASE_DIR}/data/ads_with_strategies.csv"

# =========================
# MANUAL LABEL MAPPING
# =========================
cluster_labels = {
    0: "Food, Lifestyle & Consumer Branding",
    1: "Retail, Utility & Promotional Messaging",
    2: "Automotive Product Marketing",
    3: "Public Awareness, Safety & Social Impact"
}

strategy_group_map = {
    "Food, Lifestyle & Consumer Branding": "Brand Storytelling",
    "Retail, Utility & Promotional Messaging": "Sales-driven",
    "Automotive Product Marketing": "Product-focused",
    "Public Awareness, Safety & Social Impact": "Purpose-driven"
}


def load_and_prepare_data():
    ads = pd.read_csv(ADS_PATH)
    nlp = pd.read_csv(NLP_PATH)

    # avoid duplicate ocr_text
    nlp = nlp.drop(columns=["ocr_text"], errors="ignore")

    # merge
    df = ads.merge(nlp, on="ad_id", how="inner")

    # cleaning
    df = df[df["ocr_word_count"] > 5]
    df = df[df["ocr_confidence_avg"] > 50]
    df = df[df["top_keywords"].notna()]

    df = df.reset_index(drop=True)

    # text cleaning
    df["ocr_text"] = df["ocr_text"].fillna("").astype(str)
    df["ocr_text"] = df["ocr_text"].str.replace(r"\b(www|com|http)\b", "", regex=True)
    df["ocr_text"] = df["ocr_text"].str.replace(r"\d+", "", regex=True)

    return df


def build_features(df):
    vectorizer = TfidfVectorizer(
        max_features=100,
        stop_words="english",
        min_df=5
    )

    tfidf_matrix = vectorizer.fit_transform(df["ocr_text"])

    svd = TruncatedSVD(n_components=50, random_state=42)
    X_reduced = svd.fit_transform(tfidf_matrix)

    numeric_features = df[
        ["sentiment_polarity", "sentiment_subjectivity"]
    ].fillna(0).values

    X = np.hstack((X_reduced, numeric_features))

    return X


def cluster_ads(df, X):
    kmeans = KMeans(n_clusters=4, random_state=42)
    df["cluster"] = kmeans.fit_predict(X)

    score = silhouette_score(X, df["cluster"])
    print("Silhouette score:", score)

    return df, score


def apply_manual_labels(df):
    df["strategy"] = df["cluster"].map(cluster_labels)
    df["strategy_group"] = df["strategy"].map(strategy_group_map)
    return df


def inspect_clusters(df):
    for c in sorted(df["cluster"].unique()):
        print(f"\nCluster {c}")
        print(df[df["cluster"] == c]["top_keywords"].head(10))
        print("\nSample OCR text:")
        print(df[df["cluster"] == c][["ocr_text"]].head(3))


def get_brand_by_strategy(df, strategy_name):
    target_cluster = None
    for k, v in cluster_labels.items():
        if v == strategy_name:
            target_cluster = k
            break

    if target_cluster is None:
        return "Strategy not found."

    brands = df[df["cluster"] == target_cluster]["competitor"].value_counts()

    if len(brands) == 0:
        return "No brands found."

    top_brand = brands.index[0]
    return f"{top_brand} frequently uses {strategy_name}"


def analyze_industry(df, industry):
    subset = df[df["competitor"] == industry]

    if len(subset) == 0:
        return "No ads found."

    dominant_cluster = subset["cluster"].value_counts().idxmax()
    strategy = cluster_labels[dominant_cluster]

    return f"{industry} ads primarily use: {strategy}"


def suggest_competitor_move(df, industry):
    result = analyze_industry(df, industry)

    if "Social Impact" in result:
        return result + "\nSuggested move: Differentiate with emotional storytelling."
    elif "Retail" in result:
        return result + "\nSuggested move: Compete with premium positioning."
    elif "Brand" in result:
        return result + "\nSuggested move: Focus on experiential branding."
    else:
        return result + "\nSuggested move: Use stronger value-based messaging."


def run_clustering():
    print("Running strategy clustering...")

    df = load_and_prepare_data()
    print("Rows after cleaning:", len(df))
    print("Columns:")
    print(df.columns)

    X = build_features(df)
    print("Feature matrix shape:", X.shape)

    df, score = cluster_ads(df, X)
    df = apply_manual_labels(df)

    inspect_clusters(df)

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved clustered dataset to: {OUTPUT_PATH}")

    # quick demos
    print("\n=== Demo outputs ===")
    print(get_brand_by_strategy(df, "Public Awareness, Safety & Social Impact"))
    print(analyze_industry(df, "chips"))
    print(analyze_industry(df, "safety"))
    print(analyze_industry(df, "beauty"))
    print(suggest_competitor_move(df, "beauty"))

    print("\nUnique industries:")
    print(df["competitor"].dropna().unique()[:50])


if __name__ == "__main__":
    run_clustering()
