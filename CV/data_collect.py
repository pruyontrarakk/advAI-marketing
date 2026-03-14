#!/usr/bin/env python
# coding: utf-8

# In[71]:


'''
Reads Pitt Ads JSON annotations, filters to '10/' folder images,
cross-references topics/sentiments, assigns competitor labels,
and outputs a master CSV ready for DB ingestion.
'''


# In[62]:


import json
import os
import csv
import re
from collections import defaultdict


# In[63]:


# CONFIGURE PATHS 
BASE_DIR = '.'
IMAGE_DIR = os.path.join(BASE_DIR,'10')
SYMBOLS_JSON = os.path.join(BASE_DIR,'Symbols.json')
TOPICS_JSON = os.path.join(BASE_DIR,'Topics.json')
SENTIMENTS_JSON = os.path.join(BASE_DIR,'Sentiments.json')
TOPICS_LIST = os.path.join(BASE_DIR,'Topics_List.txt')
SENTIMENTS_LIST = os.path.join(BASE_DIR,'Sentiments_List.txt')
OUTPUT_CSV = './collected_ads.csv'


# In[64]:


# UNCLEAR CATEGORY
EXCLUDED_TOPIC = '39'


# In[65]:


# PARSE TOPICS_LIST/ CATEGORY LIST
def load_topics_list(path):
    topics = {}
    with open(path, 'r', encoding = 'utf-16') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            match = re.match(r'^(\d+)\s+"([^"]+)"\s+\(ABBREVIATION:\s+"([^"]+)"\)',line)
            if match:
                idx, full, abbr = match.group(1), match.group(2), match.group(3)
                topics[idx] = {'full': full, 'abbreviation': abbr.lower()}
        return topics


# In[66]:


# PARSE SENTIMENTS_LIST
def load_sentiments_list(path):
    sentiments = {}
    with open(path, 'r', encoding = 'latin-1') as f:
        for line in f:
            line = line.strip()
            line = re.sub(r'\s+', '', line)

            if not line: continue
            # match = re.match(r'^(\d+)\.\s+"([^"]+)"\s+\(ABBREVIATION:\s+"([^"]+)"\)',line)
            match = re.match(r'^(\d+)\."([^"]+)"\(ABBREVIATION:"([^"]+)"\)',line)
            if match:
                idx, full, abbr = match.group(1), match.group(2), match.group(3)
                sentiments[idx] = {'full': full, 'abbreviation': abbr.lower()}
        return sentiments


# In[67]:


# RESOLVE IMAGE PATH
def resolve_image_path(json_key):
    '''
    json_key examples: '10/170741.png', '10/170489.jpg'
    Returns absolute image path
    '''
    filename = os.path.basename(json_key)
    full_path = os.path.join(IMAGE_DIR, filename)
    if os.path.exists(full_path): return full_path
    return None


# In[68]:


topics_meta = load_topics_list(TOPICS_LIST)
topics_meta['2']


# In[69]:


# MAIN COLLECTION LOGIC
def collect():
    print('Loading files..')
    topics_meta = load_topics_list(TOPICS_LIST)
    sentiments_meta = load_sentiments_list(SENTIMENTS_LIST)

    with open(SYMBOLS_JSON, 'r') as f: symbols_data = json.load(f)
    with open(TOPICS_JSON, 'r') as f: topics_data = json.load(f)
    with open(SENTIMENTS_JSON, 'r') as f: sentiments_data = json.load(f)

    # using only '10/' folder only
    desired_keys = [k for k in symbols_data if k.startswith('10/')]
    print(f'Images found in "10/" folder is {len(desired_keys)}')

    records = []
    # obj_records = []

    for key in desired_keys:
        # category resolution
        raw_topics = topics_data.get(key,[])

        # deduplicate
        unique_topics = list(set(raw_topics))

        # skip the excluded category
        non_unclear = [t for t in unique_topics if t!=EXCLUDED_TOPIC]
        if not non_unclear: continue

        unique_topics = non_unclear

        # sentiments resolution
        raw_sents = sentiments_data.get(key,[])
        sents = []
        for s in raw_sents: sents.extend(s)
        unique_sents = list(set(sents))

        # image path
        image_path = resolve_image_path(key)
        if image_path is None:
            print(f'No image of {key} found in image folder')
            continue
        # build ad_id
        ad_id = os.path.splitext(os.path.basename(key))[0]

        # Category labels
        topic_abbr = [topics_meta.get(t,{}).get('abbreviation',f'topic_{t}') for t in unique_topics]
        topic_full = [topics_meta.get(t,{}).get('full',f'topic_{t}') for t in unique_topics]

        primary_topic_abbr = topic_abbr[0] if topic_abbr else 'Unknown'

        # Sentiment labels
        sentiment_abbr = [sentiments_meta.get(s,{}).get('abbreviation',f'sentiment_{s}') for s in unique_sents]
        sentiment_full = [sentiments_meta.get(s,{}).get('full',f'sentiment_{s}') for s in unique_sents]

        # competitor labels
        competitor = primary_topic_abbr

        # objects and symbols resolution
        obj=[]
        for lst in symbols_data[key]:
            if len(lst)==5: obj.append(lst[-1])


        # records
        records.append({
            'ad_id':ad_id,
            'json_key':key,
            'image_path':image_path,
            'competitor': competitor,
            'all_categories':'|'.join(topic_abbr),
            'all_categories_full':'|'.join(topic_full),
            'all_sentiments':'|'.join(sentiment_abbr),
            'all_sentiments_full': '|'.join(sentiment_full),
            'objects_symbols': '|'.join(obj)
        })

    # writing csv 
    if records:
        cols = records[0].keys()
        with open(OUTPUT_CSV, 'w', newline = '', encoding = 'utf-8') as f:
            writer = csv.DictWriter(f, fieldnames = cols)
            writer.writeheader()
            writer.writerows(records)
        print(f'collected_ads.csv has {len(records)} ads')
    else:
        print('No records collected')

    return records








# In[70]:


if __name__=='__main__':
    collect()


# In[ ]:




