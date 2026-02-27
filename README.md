# NLP — Image Text Analysis

Analyzes OCR text from `collected_ads_enriched.csv` for each ad image, given that OCR text has at least 3 words extracted.

## Outputs

| Column | Description |
|--------|-------------|
| `sentiment_polarity` | -1 (negative) to 1 (positive) |
| `sentiment_subjectivity` | 0 (factual) to 1 (opinion) |
| `top_words` | Most frequent words (stopwords removed) |
| `top_keywords` | TF-IDF keywords per ad |

## How to Run

```
python3 -m pip install pandas textblob nltk scikit-learn 
```

```
python3 nlp/analyze_image_text.py
```
