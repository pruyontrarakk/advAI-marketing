import sqlite3
import pandas as pd

INPUT = "/content/drive/MyDrive/Colab Notebooks/intelligence_ai_marketing/data/ads_with_strategies.csv"
DB_PATH = "/content/drive/MyDrive/Colab Notebooks/intelligence_ai_marketing/database/ads.db"

df = pd.read_csv(INPUT)

conn = sqlite3.connect(DB_PATH)

df.to_sql("ads", conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print("Database created with", len(df), "ads")
