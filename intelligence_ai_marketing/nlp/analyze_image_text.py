"""
Analyzes OCR text from collected_ads_enriched.csv:
- Sentiment
- Top words (by frequency, stopwords removed)
- Top keywords (TF-IDF)
"""

import pandas as pd
from collections import Counter
from pathlib import Path

# NLP imports (install: pip install textblob nltk scikit-learn)
from textblob import TextBlob
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer

# Download NLTK data once
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = PROJECT_ROOT / 'collected_ads_enriched.csv'
OUTPUT_CSV = PROJECT_ROOT / 'nlp' / 'image_text_analysis.csv'

# Config
MIN_WORDS = 3  # Skip rows with fewer words
TOP_N_WORDS = 3
TOP_N_KEYWORDS = 3
STOP_WORDS = set(stopwords.words('english'))


def get_sentiment(text: str) -> dict:
    """Returns polarity (-1 to 1) and subjectivity (0 to 1)."""
    blob = TextBlob(text)
    return {
        'polarity': round(blob.sentiment.polarity, 4),
        'subjectivity': round(blob.sentiment.subjectivity, 4),
    }


def get_top_words(text: str, n: int = TOP_N_WORDS) -> list:
    """Tokenize, remove stopwords, return top N by frequency."""
    words = word_tokenize(text.lower())
    words = [w for w in words if w.isalnum() and w not in STOP_WORDS and len(w) > 1]
    counts = Counter(words)
    return [word for word, _ in counts.most_common(n)]


def get_top_keywords_for_all(texts: list, n: int = TOP_N_KEYWORDS) -> list[list[str]]:
    """Fit TF-IDF once, return top N keywords per document."""
    if not texts:
        return []
    vec = TfidfVectorizer(max_features=500, stop_words='english')
    tfidf = vec.fit_transform(texts)
    feature_names = vec.get_feature_names_out()
    results = []
    for idx in range(len(texts)):
        if not texts[idx].strip():
            results.append([])
            continue
        row = tfidf[idx].toarray().flatten()
        top_indices = row.argsort()[-n:][::-1]
        keywords = [feature_names[i] for i in top_indices if row[i] > 0]
        results.append(keywords)
    return results


def main():
    df = pd.read_csv(INPUT_CSV)
    # Filter: only rows with meaningful OCR text
    mask = df['ocr_text'].notna() & (df['ocr_word_count'] >= MIN_WORDS)
    df_text = df[mask].copy()

    texts = df_text['ocr_text'].fillna('').astype(str).tolist()

    # Fit TF-IDF once (avoids 4k+ fits and memory blowup)
    print("Computing keywords (TF-IDF)...")
    all_keywords = get_top_keywords_for_all(texts)

    results = []
    for pos, (_, row) in enumerate(df_text.iterrows()):
        text = row['ocr_text']
        sentiment = get_sentiment(text)
        top_words = get_top_words(text)
        top_keywords = all_keywords[pos]

        results.append({
            'ad_id': row['ad_id'],
            'ocr_text': text[:200] + '...' if len(text) > 200 else text,
            'sentiment_polarity': sentiment['polarity'],
            'sentiment_subjectivity': sentiment['subjectivity'],
            'top_words': '|'.join(top_words),
            'top_keywords': '|'.join(top_keywords),
        })

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(out_df)} rows to {OUTPUT_CSV}")


if __name__ == '__main__':
    main()