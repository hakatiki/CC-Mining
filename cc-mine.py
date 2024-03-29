import datetime
import pandas as pd
from tqdm import tqdm
from time import sleep
import wget
import os
import gc
import gzip
import sys


TEXT_MODE = False
INDEX_BEGIN = 0
TEXT_BEGIN = 0
TXT_IDX = 0
for i in sys.argv:
    if "text_mode" in i:
        TEXT_MODE = True
    if "continue_index" in i:
        INDEX_BEGIN = int(i.split("=")[-1])
    if "continue_text" in i:
        TEXT_BEGIN = int(i.split("=")[-1])
    if "txt_idx" in i:
        TXT_IDX = int(i.split("=")[-1])
    

PATH = 'https://data.commoncrawl.org/'
LANG = "hu"
CC_LANG = "hun"
INDEX_FILENAME = "hungarian.csv"

urls = pd.read_csv('cc-index-table.paths',header=None)


def wget_file(path, current_file=None):
    try:
        if current_file is not None:
            wget.download(path, out=current_file)
        else:
            wget.download(path)
        return True
    except:
        print('Error downloading', path)
        
        return False

def warc_2_wet(base, filename):
    filename = filename.replace('/warc/', '/wet/')
    filename = filename.replace('.warc.gz', '.warc.wet.gz')
    return base + filename

def get_hungarian_from_wet(filecontent, db, target_lang=CC_LANG):
    idx = 0
    contents = []
    can_save = True
    while idx < len(filecontent):
        if filecontent[idx].startswith("WARC-Target-URI:"):
            current_url = filecontent[idx].split(" ")[-1]
            if current_url in db:
                can_save = False
            else:
                can_save = True
                db.add(current_url)
        tmp_contents = []
       
        if can_save and filecontent[idx].startswith("WARC-Identified-Content-Language:") and target_lang in filecontent[idx]:
            seen = set()
            # print(filecontent[idx+1])
            if "Content-Type: text/plain" in filecontent[idx+1]:
                # print(filecontent[idx])
                idx = idx + 3
                while not (filecontent[idx] == "" and filecontent[idx+1] == "" ) :
                    if len(filecontent[idx]) > 60 and filecontent[idx] not in seen:
                        tmp_contents.append(filecontent[idx])
                        seen.add(filecontent[idx])
                    idx +=1
                tmp_contents.append("")
                tmp_contents.append("")
        
        if (tmp_contents != []):
            # print(idx)
            contents.extend(tmp_contents)
            # gc.collect()
        idx += 1

    return contents, db
if not TEXT_MODE:
    for i in range(INDEX_BEGIN, len(urls)):
        current_file = 'current_index.gz.parquet'
        filename = urls[0][i]
        url = PATH + filename
        gc.collect()
        if wget_file(url, current_file):
            print(f"Downloaded {filename}")
            df = []
            gc.collect()
            df = pd.read_parquet(current_file)  
            gc.collect()
            df = df[df['content_languages'].str.startswith('hu', na=False)]
            if (i == 0):
                df.to_csv('hungarian.csv', header=True, index=False)
            else:
                df.to_csv('hungarian.csv', mode='a', header=False, index=False)
            print(f"Saved {len(df)} rows to file")
            os.remove(current_file)
            print("Removed current file")


print("------------------------------------")
print("Finished downloading index files")
print("------------------------------------")

if not os.path.exists("hungarian_sorted.csv"):
    gc.collect()
    hungarian_index = pd.read_csv('hungarian.csv', low_memory=False, usecols=['warc_filename', 'url_host_name'])
    print("Loaded data")
    # hungarian_index = hungarian_index.loc[:, ['warc_filename', 'url_host_name']]
    gc.collect()
    hungarian_sorted = hungarian_index.groupby(['warc_filename'])['url_host_name'].count().reset_index(name='count').sort_values(['count'], ascending=False)
    print(f"Unique sites: {len(hungarian_sorted)}")
    hungarian_sorted.to_csv("hungarian_sorted.csv", index=False)
else:
    hungarian_sorted = pd.read_csv("hungarian_sorted.csv")
    print("Loaded sorted data")

temp_file_name = "current_wet_tmp.warc.wet.gz"
text_buffer = []
current_count = 0
total_count = 0
file_index = TXT_IDX
db = set()
        
for i in range(TEXT_BEGIN, len(hungarian_sorted)):
    filename = hungarian_sorted["warc_filename"][i]
    print("Index state: ", i)
    url = warc_2_wet(PATH, filename)
    print(f"Downloading {url}")
    if wget_file(url, temp_file_name):
        print(f"Downloaded {filename}")
        with gzip.open(temp_file_name, 'rb') as f:
            current_file_content = f.read().decode('utf-8').splitlines()
        print(f"Read {len(current_file_content)} lines")
        hungarian_text, db = get_hungarian_from_wet(current_file_content, db,)
        gc.collect()
        print(f"Found {len(hungarian_text)} lines")

        text_buffer.extend(hungarian_text)
        total_count += len(hungarian_text)
        current_count += len(hungarian_text)
        # current time
        print("Current Time =", datetime.datetime.now())
        print("Current count: ", current_count)
        print("Total count: ", total_count)
        if current_count > 1_000_000:
            # text_buffer = list(set(text_buffer))
            # write to file
            with open(f"./data/hungarian_text_{file_index}.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(text_buffer))
            file_index += 1
            current_count = 0
            text_buffer = []
            gc.collect()
            print(f"Saved in total: {total_count} lines to files")
        gc.collect()
        os.remove(temp_file_name)
        print(f"Removed {temp_file_name}")