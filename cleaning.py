import numpy as np
from Levenshtein import distance
from tqdm import tqdm# load ./data/hungarian_data_0.txt
with open('./data/hungarian_text_0.txt', encoding="utf-8") as f:
    data = f.readlines()
def remove_near_duplicates(strings):
    # Create an empty list to store unique strings
    unique_strings = []
    duplicates = []
    
    # Loop through each string in the input list
    for string in strings:
        # Check if the string is similar to any existing unique strings
        is_duplicate = False
        if string == '\n':
            unique_strings.append(string)
            continue
        for unique_string in unique_strings:
            # Calculate the Levenshtein distance between the strings
            lev_distance = distance(string, unique_string)
            
            # Check if the distance is below the given threshold
            threshold = min(len(unique_string), len(string))//2
            if lev_distance <= threshold:
                # print("--------------------------------------------")
                # print(unique_string)
                # print(string)
                is_duplicate = True
                break
        
        # If the string is not a near duplicate, add it to the list of unique strings
        if not is_duplicate:
            unique_strings.append(string)
        else:
            duplicates.append(string)
    
    # Return the list of unique strings
    return unique_strings, duplicates

from concurrent.futures import ThreadPoolExecutor

def remove_near_duplicates_wrapper(data):
    return remove_near_duplicates(data)

WINDOW = 10000
num_processes=16
executor = ThreadPoolExecutor(max_workers=num_processes)
futures = []

for i in range(0, len(data), WINDOW):
    subset = data[i:i+WINDOW].copy()
    future = executor.submit(remove_near_duplicates_wrapper, subset)
    futures.append(future)
print("setup complete")
unduped = []
for future in futures:
    unique, dupes = future.result()
    unduped.extend(unique)
