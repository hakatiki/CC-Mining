import numpy as np
from Levenshtein import distance
from tqdm import tqdm
import os


WINDOW = 10000
paths = os.listdir('./data')
paths = [path for path in paths if path.endswith('.txt')]

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


for i in paths:
    with open('./data/'+i, encoding="utf-8") as f:
        data = f.readlines()
    unduped = []
    for i in range(0, len(data),WINDOW ):
        unique, dupes = remove_near_duplicates(data[i:i+WINDOW])
        unduped.extend(unique)
    with open('./dedup-data/'+i, encoding="utf-8") as f:
        f.write(unduped)
