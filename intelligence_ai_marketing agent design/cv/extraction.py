#!/usr/bin/env python
# coding: utf-8

# In[12]:


import subprocess
subprocess.run(["jupyter", "nbconvert", "--to", "script", "extraction.ipynb"])


# In[13]:


import os
from PIL import Image
import csv
from multiprocessing import Pool
from helpers import process_row, NEW_COLUMNS


# In[14]:


INPUT_CSV = 'collected_ads.csv'
OUTPUT_CSV = 'collected_ads_enriched.csv'
NUM_WORKERS = 4


# In[15]:


def extract_all():
    with open(INPUT_CSV, 'r', encoding = 'utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        original_fields = reader.fieldnames

    total = len(all_rows)
    print(f'Total Images to process {total}')

    # Process all rows in parallel

    with Pool(processes = NUM_WORKERS) as pool :
        enriched_rows = []
        for idx, row in enumerate(pool.imap(process_row, all_rows), 1):
            enriched_rows.append(row)

            if idx%500==0:
                done_s = sum(1 for r in enriched_rows if r["extraction_status"] == "success")
                done_f = sum(1 for r in enriched_rows if r["extraction_status"].startswith("failed"))
                done_k = idx - done_s - done_f
                print(f"  [{idx:>5}/{total}] {done_s} |  {done_f} |  {done_k}")




    final_fields = original_fields + [c for c in NEW_COLUMNS if c not in original_fields]
    with open(OUTPUT_CSV, 'w', newline='',encoding = 'utf-8') as f:
        writer = csv.DictWriter(f, fieldnames = final_fields)
        writer.writeheader()
        writer.writerows(enriched_rows)

    success = sum(1 for r in enriched_rows if r["extraction_status"] == "success")
    failed  = sum(1 for r in enriched_rows if r["extraction_status"].startswith("failed"))
    skipped = total - success - failed

    print('Final Summary')
    print(f'Total {total}')
    print(f'Success {success}')
    print(f'Failed {failed}')
    print(f'Skipped {skipped}')
    print('Enriched CSV saved')



# In[16]:


if __name__ == '__main__':
    extract_all()


# In[ ]:




