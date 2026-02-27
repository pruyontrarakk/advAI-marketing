#!/usr/bin/env python
# coding: utf-8

# In[10]:


import sqlite3
import csv
import os


# In[11]:


DB_PATH = 'ads.sqlite'
ADS_CSV = 'collected_ads_enriched.csv'


# In[12]:


def read_csv(path):
    if not os.path.exists(path):
        print(f'CSV not found {path}')
        return []
    with open(path, 'r', encoding = 'utf-8') as f:
        return list(csv.DictReader(f))


# In[37]:


def ingest_ads(conn, rows):
    '''
    Insert ad rows from csv
    '''
    cur = conn.cursor()
    inserted = 0

    for row in rows:
        ad_id = row["ad_id"].strip()
        cur.execute(
            '''
            INSERT OR IGNORE INTO ads (
            ad_id,json_key,image_path,competitor,objects_symbols,image_width,image_height,
            image_format,ocr_text,ocr_word_count,ocr_confidence_avg,text_area_px,image_area_px,
            text_image_ratio,layout_type,dominant_color_1,dominant_color_2,dominant_color_3,
            dominant_color_4,dominant_color_5,color_palette_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''',(
                ad_id,
                row['json_key'].strip(),
                row['image_path'].strip(),
                row['competitor'].strip(),
                row['objects_symbols'].strip(),
                int(row['image_width']),
                int(row['image_height']),
                row['image_format'].strip(),
                row.get('ocr_text','').strip(),
                int(row['ocr_word_count']),
                float(row['ocr_confidence_avg']),
                int(row['text_area_px']),
                int(row['image_area_px']),
                float(row['text_image_ratio']),
                row['layout_type'].strip(),
                row['dominant_color_1'].strip(),
                row['dominant_color_2'].strip(),
                row['dominant_color_3'].strip(),
                row['dominant_color_4'].strip(),
                row['dominant_color_5'].strip(),
                row['color_palette_json'].strip()
            )
        )
        inserted += cur.rowcount

        # ad_categories

        categories_abbrs = row['all_categories'].split('|') if row['all_categories'] else []
        categories_fulls = row['all_categories_full'].split('|') if row['all_categories_full'] else []

        for i, cabbr in enumerate(categories_abbrs):
            full = categories_fulls[i] if i<len(categories_fulls) else ''
            cur.execute(
                '''
                INSERT OR IGNORE INTO ads_categories(
                ad_id,category_abbr,category_full,is_primary
                )
                VALUES(?,?,?,?)
                ''',(ad_id,cabbr,full,1 if i==0 else 0)
            )

        # ad_sentiments

        sentiments_abbrs = row['all_sentiments'].split('|') if row['all_sentiments'] else []
        sentiments_fulls = row['all_sentiments_full'].split('|') if row['all_sentiments_full'] else [] 

        for i, sabbr in enumerate(sentiments_abbrs):
            full = sentiments_fulls[i] if i<len(sentiments_fulls) else ''
            cur.execute(
                '''
                INSERT OR IGNORE INTO ads_sentiments(
                ad_id,sentiment_abbr,sentiment_full
                )
                VALUES(?,?,?)
                ''',(ad_id,sabbr,full)
            )
    return inserted




# In[17]:


def fill_database():
    if not os.path.exists(DB_PATH): 
        print(f'DB not found at {DB_PATH}')
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    print('Reading ads csv...')

    rows = read_csv(ADS_CSV)

    print('Inserting into database...')
    n_ads = ingest_ads(conn,rows)
    conn.commit()

    print(f'Number of insertions in core ads table is {n_ads}')

    print('Testing...')

    for table in ['ads','ads_categories','ads_sentiments']:
        try:
            count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            print(f'{table} {count} rows')
        except Exception as e:
            print(f'{table} error {e}')

    print('Testing ads per category...')

    for row in conn.execute('''
    SELECT competitor, COUNT(*) as n
    FROM ads GROUP BY competitor ORDER BY n DESC
    '''):
        print(f'{row[0]} {row[1]} ads')

    print('Testing top categories...')

    for row in conn.execute(
        '''
        SELECT category_abbr, COUNT(*) AS n
        FROM ads_categories GROUP BY category_abbr ORDER BY n DESC LIMIT 8
        '''
    ):
        print(f'{row[0]} {row[1]}')

    print('Testing top sentiments...')

    for row in conn.execute(
        '''
        SELECT sentiment_abbr, COUNT(*) AS n
        FROM ads_sentiments GROUP BY sentiment_abbr ORDER BY n DESC LIMIT 8
        '''
    ):
        print(f'{row[0]} {row[1]}')

    conn.close()
    print('Database saved')





# In[39]:


if __name__=='__main__':
    fill_database()


# In[38]:


import gc

# Close any named connection
try:
    conn.close()
    print("conn closed")
except:
    pass

# Force close ALL open sqlite connections in this kernel
for obj in gc.get_objects():
    if type(obj).__name__ == "Connection":
        try:
            obj.close()
            print(f"closed: {obj}")
        except:
            pass


# In[ ]:




