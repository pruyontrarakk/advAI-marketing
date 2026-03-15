#!/usr/bin/env python
# coding: utf-8

# In[34]:


import subprocess
subprocess.run(["jupyter", "nbconvert", "--to", "script", "helpers.ipynb"])


# In[33]:


import json
import os
from PIL import Image
import pytesseract
import colorthief as ct


# In[35]:


get_ipython().system('pip install pytesseract pillow colorthief opencv-python')


# In[27]:


# LAYOUT THRESHOLDS
TEXT_HEAVY_THRESHOLD = 0.40 # > 40% of image area is text--text_heavy
IMAGE_HEAVY_THRESHOLD = 0.20 # < 20%  of image area is text--image_heavy
# Between 20–40% -- balanced


# In[28]:


# DOMINANT COLOR EXTRACTION
def rgb_to_hex(rgb_tuple):
    ''' Convert (R,G,B) to #RRGGBB'''
    return '#{:02X}{:02X}{:02X}'.format(*rgb_tuple)

def get_dominant_colors(image_path, n_colors = 5):
    '''
    Returns list of hex color strings, most dominant first
    Uses colorthief which runs k-means on the image pixels 
    '''
    try:
        thief = ct.ColorThief(image_path)
        if n_colors ==1: palette = [thief.get_color(quality = 1)]
        else: palette = thief.get_palette(color_count = n_colors, quality = 1)
        return [rgb_to_hex(c) for c in palette]
    except Exception:
        return []



# In[29]:


# OCR + LAYOUT ANALYSIS
def extract_ocr_and_layout(image_path):
    '''
    Returns dict with:
      ocr_text, ocr_word_count, ocr_confidence_avg,
      text_area_px, image_area_px, text_image_ratio, layout_type
    '''

    img = Image.open(image_path).convert('RGB')
    img_w, img_h = img.size
    image_area_px = img_w * img_h

    # pytesseract word-level data
    data = pytesseract.image_to_data(img, output_type = pytesseract.Output.DICT)

    words = []
    confidences = []
    text_area = 0
    for i in range(len(data['text'])):
        word = data['text'][i].strip()
        conf = int(data['conf'][i])

        if conf < 40: continue 
        if not word: continue # blank string
        words.append(word)
        confidences.append(conf)

        w = data['width'][i]
        h = data['height'][i]
        text_area += w*h

    ocr_text = ' '.join(words)
    ocr_word_count = len(words)
    ocr_conf_avg = round(sum(confidences) / len(confidences), 2) if confidences else 0.0
    text_image_ratio = round(text_area / image_area_px, 4) if image_area_px > 0 else 0.0

    # Layout classification
    if text_image_ratio >= TEXT_HEAVY_THRESHOLD: layout_type = 'text_heavy'
    elif text_image_ratio <= IMAGE_HEAVY_THRESHOLD: layout_type = 'image_heavy'
    else: layout_type = 'balanced'

    return {
        'image_width'       :img_w,
        'image_height'      :img_h,
        'image_format'      :img.format if img.format else 'UNKNOWN',
        'ocr_text'          :ocr_text,
        'ocr_word_count'    :ocr_word_count,
        'ocr_confidence_avg':ocr_conf_avg,
        'text_area_px'      :text_area,
        'image_area_px'     :image_area_px,
        'text_image_ratio'  :text_image_ratio,
        'layout_type'       :layout_type
    }



# In[30]:


# PER IMAGE PROCESSING
def process_image(image_path):
    '''
    Runs OCR + color extraction on one image.
    Returns a merged feature dict
    '''
    Image.open(image_path).verify()

    layout_data = extract_ocr_and_layout(image_path)
    colors = get_dominant_colors(image_path, n_colors = 5)

    # Pad colors list to have 5 slots
    while len(colors) < 5: colors.append(None)

    return{
        **layout_data,
        'dominant_color_1' :colors[0],
        'dominant_color_2' :colors[1],
        'dominant_color_3' :colors[2],
        'dominant_color_4' :colors[3],
        'dominant_color_5' :colors[4],
        'color_palette_json' : json.dumps([c for c in colors if c])
    }


# In[31]:


NEW_COLUMNS = [
    "image_width", "image_height", "image_format",
    "ocr_text", "ocr_word_count", "ocr_confidence_avg",
    "text_area_px", "image_area_px", "text_image_ratio", "layout_type",
    "dominant_color_1", "dominant_color_2", "dominant_color_3",
    "dominant_color_4", "dominant_color_5", "color_palette_json",
    "extraction_status"   # 'success' or 'failed' 
]


# In[32]:


def process_row(row):
    '''
    Called by each worker process independently.
    Returns the enriched row dict.
    '''
    image_path = row.get('image_path','').strip()

    # if no image
    if not image_path or not os.path.exists(image_path):
        for col in NEW_COLUMNS: row[col] = ''
        row['extraction_status'] = 'skipped'

        return row

    # # skip if already processed
    # if row.get('extraction_status') == 'success':

    #     return row

    try:
        features = process_image(image_path)


        for col in NEW_COLUMNS:
            row[col] = features.get(col,'')
        row['extraction_status'] = 'success'




    except Exception as e:
        for col in NEW_COLUMNS:
            row[col] = ''
        row['extraction_status'] = f'failed : {str(e)[:80]}'
    return row


# In[ ]:





# In[ ]:




