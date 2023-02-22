import pandas as pd
from tqdm import tqdm
from time import sleep
import wget


path = 'https://data.commoncrawl.org/'
urls = pd.read_csv('cc-index-table.paths',header=None)

def get_index(path, current_file=None):
    try:
        if current_file is not None:
            wget.download(path, out=current_file)
        else:
            wget.download(path)
    except:
        print('Error downloading', path)
        

for i in tqdm(range(urls)):
    current_file = 'current_index.gz.parquet'
    filename = urls[0][i]
    url = path + filename
    # get_index(url, current_file)
    print(f"Downloaded {filename}")
    df = pd.read_parquet(current_file)  
    df = df[df['content_languages'].str.startswith('hu', na=False)]
    df.to_csv('hungarian.csv', mode='a', header=False, index=False)
    