# Setup
import pandas as pd
import numpy as numpy
import os
import json
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Own functions
from models import query, ETL
from models.helpers import *
from utils import utils


def automatic_migrate_source2accoutn_otf(API_KEY_SOURCE:str, 
                                         API_KEY_ACCOUNT:str):
    
    keys = {
        "API_SOURCE": {"api_url": "https://api.hubapi.com/crm/v3/objects/contacts/search",
                       "api_key": API_KEY_SOURCE
                       },
        "API_ACCOUNT": {"api_url": "https://api.hubapi.com/crm/v3/objects/contacts/batch/create",
                       "api_key": API_KEY_ACCOUNT
                       },
        
    }
    
    for name, itmes in keys.items():
        assert isinstance(itmes["api_key"], str), f"{name} not is a string"
        printy(F"Review ststus {name} ...")
        if utils.check_api_status(**itmes):
            printg(f"Correctly connection with {name}")
        else:
            raise Exception(f"Error connection with {name}")

    print("** Continue for extract source data\n")
    
    # Crate checkpoint folder
    Path('checkpoint').mkdir(parents=True, exist_ok=True)
    data_path = "checkpoint/data.csv"
    printy("Start search")
    # Extract first page
    dict_data_result = query.searchAPI(keys["API_SOURCE"]["api_key"],)
    # Convert in DataFrame
    data = [ETL.dict2dataframe(dict_data_result)]
    # Partition
    data_size = dict_data_result["total"]
    iterations = data_size // 100 if data_size % 100 == 0 else (data_size // 100) + 1
    limit = 100
    for i in tqdm(range(1,iterations), desc="Extracting"):
        if data_size - (i*100) <= 100:
            limit = data_size - (i*100)
        dict_data_result = query.searchAPI(keys["API_SOURCE"]["api_key"], limit = limit, page = i)
        # Convert in DataFrame
        data.extend([ETL.dict2dataframe(dict_data_result)])
    printg("Finish search")
    # Save in checkpoint
    data = pd.concat(data)
    data.to_csv(data_path, sep = "|", index = False)



if __name__ == "__main__":

    keys_path = "keys/api_key.json"
    # Check if existing keys
    if not os.path.exists(keys_path):
        keys = {}
        while True:
            # In case it does not exist, the following are requested
            name_key = str(input("Enter name of your API key: "))
            api_key = str(input("Enter your API key token: "))

            keys[name_key] = api_key

            answer = str(input("Other API key? y/n")).lower()

            if answer in ["y", "yes", "n", "no"]:
                Path('keys').mkdir(parents=True, exist_ok=True)
                if answer in ["n", "no"]:
                    # Finally save keys
                    with open(keys_path, 'w') as file:
                        json.dump(keys, file)
                    break
            else:
                while answer not in ["y", "yes", "n", "no"]:
                    answer = str(input("Invalid answer\n. Other API key? y/n")).lower()



    with open(keys_path, 'r') as file:
        keys_data = json.load(file)
    printg("API keys loaded successfully")

    automatic_migrate_source2accoutn_otf(API_KEY_SOURCE= keys_data["data_key"],
                                         API_KEY_ACCOUNT= keys_data["account_key"])
