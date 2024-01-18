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
    print("\t Total elements", len(data) ,"\n")

    ### Transform data
    printy("Start transform data")
    ## Simple transformation
    print("\t1. simple transform")
    data["createdate"] = data["createdate"].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
    ## Found country and city
    print("\t2. find country/city")
    tqdm.pandas(desc="Applying Function")
    data[["contry_found","city_found"]] = data["country"].progress_apply(lambda x: pd.Series(ETL.found_contry(x)))

    # Both the country and number information are empty, and it is evident that the records are repeated, so they will be eliminated
    data = data[~data["country"].isna()]
    data = data.reset_index(drop= True)

    # Set Winchester
    for i in range(len(data)):
        if data.iloc[i]["country"] == "Winchester":
            data.loc.__setitem__((i, ('contry_found')), "United Kingdom")
            data.loc.__setitem__((i, ('city_found')), "Winchester")

    ## Regrex email
    print("\t3. find email")
    data["email"] = data["raw_email"].apply(lambda x: ETL.find_email(x))    

    ## Fix phone number
    # Having only two countries it is more convenient to use a dictionary to normalize than to implement some kind of external library
    dict_normalize_phones = {
        "Ireland":"(+353)",
        "United Kingdom": "(+44)"
    }

    print("\t4. normalizate phone number")
    data["fix_phone"] = data[["phone","contry_found"]].apply(lambda x: ETL.normalize_phones(
        dict_normalize_phones = dict_normalize_phones,
        phone = x.iloc[0], country = x.iloc[1]), axis = 1)

    printg("Finish processing")
    
    ### Duplicates Management
    printy("Start Duplicates Management")

    temporal_vector = []
    for email in tqdm(data["email"].unique(), desc="Extracting"):
        #print(email)
        data_tmp = data[data["email"] == email].copy()

        if len(data_tmp) != 1:
            # Find duplicate
            data_tmp = data_tmp.sort_values( by = "createdate", ascending = False).reset_index(drop = True)
            industry = ";".join(list(data_tmp["industry"].unique()))
            data_tmp.loc.__setitem__((0, ('industry')), industry)
            temporal_vector.append(data_tmp.head(1))
        else:
            temporal_vector.append(data_tmp)

    data_postprocessing = pd.concat(temporal_vector)

    # This record is deleted because we do not have complete information, although it can be assumed that his name is tyson and 
    # his last name is newman and his email address, it is decided not to display any information
    data_postprocessing = data_postprocessing[~data_postprocessing["firstname"].isna()].reset_index(drop=True)

    printg("Finish processing")

    ### Saving data
    printy("Start save in Account")
    data_postprocessing['city_found'] = data_postprocessing['city_found'].fillna('')
    data_postprocessing["hs_object_id"] = data_postprocessing["hs_object_id"].astype(int)
    data_send = data_postprocessing[
        [
            "firstname", "lastname","email", "fix_phone", "contry_found", "city_found",
            "technical_test___create_date", "industry", "hs_object_id"
        ]
    ].copy()

    data_send.columns = [
        "firstname", "lastname","email", "phone", "country", "city",
        "original_create_date", "original_industry", "temporary_id"
    ]

    data_send["response_save"] = [query.sendAPI(keys["API_ACCOUNT"]["api_key"], 
                                                input_data = data_send.iloc[i]) \
                                for i in tqdm(range(len(data_send)))]
    
    data_send[data_send["response_save"] == "error"].to_csv("checkpoint/data_error_in_save.csv",
                                                            index= False, sep = "|")

    value_counts_result = data_send["response_save"].value_counts()
    
    values = value_counts_result.values
    index = value_counts_result.index

    printg("Finish save\n")

    print("Summary process save data:")
    for i, v in zip(index, values):
        print(f"\n{i}, Count: {v}")

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
