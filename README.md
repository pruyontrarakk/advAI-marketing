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


# Strategy Clustering, Analytics, and AI Marketing Assistant

In addition to the CV and NLP pipelines above, this repository also contains a complete downstream system that transforms enriched advertisement data into an AI-powered marketing intelligence assistant.

This part of the project integrates strategy clustering, dataset analytics, a Retrieval-Augmented Generation (RAG) system, and a lightweight interactive frontend demo.

Together, the folders in this repository form a full **end-to-end pipeline**, starting from computer vision feature extraction and ending with an AI assistant that generates marketing insights and advertising concepts.

---

## Pipeline Overview

After OCR text, visual features, and metadata are extracted from advertisements, the system performs several additional stages:

### 1. Strategy Clustering

Advertisements are grouped into strategic categories using an unsupervised clustering pipeline.

Steps include:

- TF-IDF vectorization of OCR text
- Dimensionality reduction using **Truncated SVD**
- Combining text features with **sentiment polarity and subjectivity**
- Applying **K-means clustering** to discover strategy groups

After clustering, representative keywords and example advertisements are manually inspected to assign meaningful strategy labels such as:

- Retail & promotional messaging
- Brand storytelling / lifestyle branding
- Public awareness campaigns
- Behavioral or action-oriented messaging

The resulting dataset is saved as:

```
data/ads_with_strategies.csv
```

---

### 2. Analytics Layer

The analytics module summarizes patterns in advertising strategies across industries.

Example insights produced by this layer include:

- dominant strategies used in different industries
- distribution of strategy categories
- relationships between industries and messaging approaches
- competitive strategy gaps

These analytics summaries are also used as contextual knowledge for the AI assistant.

---

### 3. Retrieval-Augmented Marketing Assistant (RAG)

To make the dataset searchable and interactive, a Retrieval-Augmented Generation system is implemented.

Main components include:

- **SentenceTransformers** (`all-MiniLM-L6-v2`) for advertisement embeddings
- **FAISS** for efficient vector similarity search
- **Qwen language model** for marketing analysis and recommendation generation

When a user submits a query, the system:

1. retrieves relevant advertisements from the dataset
2. combines them with analytics summaries
3. generates structured marketing insights

The assistant can perform tasks such as:

- identifying dominant strategies within an industry
- detecting competitive gaps
- generating advertising concepts
- retrieving similar advertisements for reference

---

### 4. Interactive Frontend Demo

A lightweight interactive interface is implemented using **Gradio**.

The demo allows users to interact with the marketing assistant without writing code.

Users can provide inputs such as:

- industry (e.g. beauty, automotive, snacks)
- target audience
- campaign style

The assistant can then perform several actions:

- **Analyze Strategy** — summarize strategy patterns in the selected industry  
- **Find Competitive Gap** — identify opportunities competitors may be missing  
- **Generate Ad Concept** — create a structured advertising concept  
- **Show Reference Ads** — retrieve similar advertisements from the dataset  

This interface demonstrates how the system can function as a practical marketing decision-support tool.

---

## Repository Structure

The repository now contains the complete pipeline from computer vision extraction to the AI marketing assistant frontend.

```
project_root/
│
├── cv/                 # computer vision extraction pipeline
├── nlp/                # text processing and sentiment analysis
├── clustering/         # strategy clustering and labeling
├── analytics/          # strategy statistics and dataset insights
├── rag/                # retrieval-augmented marketing assistant
├── visualization/               # interactive frontend (Gradio demo)
├── database/           # SQLite database creation and storage
├── data/               # processed datasets and intermediate outputs
│
├── collected_ads.csv
├── collected_ads_enriched.csv
├── image_text_analysis.csv
├── ads_with_strategies.csv
│
└── ads.sqlite
```

---

## Key Output Files

| File | Description |
|-----|-------------|
| `collected_ads.csv` | Raw dataset generated from JSON annotations |
| `collected_ads_enriched.csv` | Dataset enriched with OCR text, layout features, and dominant colors |
| `image_text_analysis.csv` | NLP analysis results including sentiment scores and TF-IDF keywords |
| `ads_with_strategies.csv` | Final dataset including strategy clusters and labeled strategy categories |
| `ads.sqlite` | SQLite database containing normalized advertisement records |

---

## End-to-End Pipeline

The full workflow implemented in this repository is:

```
Ad Images
   ↓
Computer Vision Extraction (OCR, layout, colors)
   ↓
NLP Analysis (sentiment + TF-IDF keywords)
   ↓
Strategy Clustering
   ↓
Analytics Layer
   ↓
RAG Marketing Assistant
   ↓
Interactive Frontend Demo
```

This pipeline transforms raw advertisement images into a structured dataset and an interactive AI assistant capable of generating marketing insights and campaign ideas.
