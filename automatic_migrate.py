# Setup
import pandas as pd
import numpy as numpy
import os
import json
from pathlib import Path
from tqdm.notebook import tqdm
from datetime import datetime
from IPython.display import clear_output

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
        printy(F"Review ststus API {name} ...")
        if utils.check_api_status(**itmes):
            printg(f"Correctly connection with {name}")
        else:
            raise Exception(f"Error connection with {name}")





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
