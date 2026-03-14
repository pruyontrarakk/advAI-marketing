# Pitt Image Ads Processing Pipeline

This repository contains  clean, runnable Python scripts that:

1. **Collect** annotated ad data from JSON files (topics, sentiments, symbols).
2. **Enrich** each ad image with OCR text, layout analysis, dominant colors, and metadata.
3. **Store** everything in a clean SQLite database (`ads.sqlite`) for easy querying and analysis.


## Installation

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/pitt-ads-pipeline.git
cd pitt-ads-pipeline
```

### 2. Install Python dependencies
```bash
pip install pillow pytesseract colorthief opencv-python
```

## Data files
Make sure these are in the root folder:

1. 10/ directory containing all ad images (the pipeline expects ~5458 images).
2. Symbols.json, Topics.json, Sentiments.json
3. Topics_List.txt (UTF-16)
4. Sentiments_List.txt (Latin-1)

## Steps to run the pipeline

1. Data collection

```bash
python data_collect.py
```
- Creates collected_ads.csv (5455 records)
- Console output: "collected_ads.csv has 5455 ads"
  
2. Feature Extraction (OCR + Colors + Layout)
   
``` bash
python extraction.py
```
- Uses 4 worker processes (fast on multi-core machines)
- Processes every image with:
- pytesseract OCR (word-level confidence)
- Text vs. image layout classification
- 5 dominant colors (via ColorThief)
- Creates collected_ads_enriched.csv
  
3. Load into SQLite Database

```bash
python data_store.py
```
- Creates ads.sqlite
- Populates three tables: ads, ads_categories, ads_sentiments
- Console output shows summary statistics

## Example Usage

1. Query the database with sqlite3

```bash
sqlite3 ads.sqlite
```

``` SQL
Example ad with OCR text
SELECT ad_id, competitor, ocr_text, dominant_color_1, layout_type 
FROM ads 
LIMIT 5;
```

2. Query with Python

```Python
import sqlite3
conn = sqlite3.connect('ads.sqlite')
for row in conn.execute("SELECT ad_id, competitor, ocr_text FROM ads LIMIT 3"):
    print(row)
conn.close()
```

3. Explore enriched CSV

Open collected_ads_enriched.csv in Excel / Pandas to see columns like:

- ocr_text, ocr_word_count
- dominant_color_1 to dominant_color_5
- text_image_ratio, layout_type (text_heavy / image_heavy / balanced)
- color_palette_json

  
## Notes & Tips

1. Performance: Step 2 (extraction) is the slowest but runs in parallel. On a modern laptop it finishes in a few minutes for 5455 images.
2. Large images: PIL warnings are expected and ignored.
3. Re-running: The database uses INSERT OR IGNORE, so it's safe to re-run.
4. Customization:
 - Change NUM_WORKERS = 4 in extraction.py
 - Adjust thresholds in helpers.py (TEXT_HEAVY_THRESHOLD, etc.)




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
