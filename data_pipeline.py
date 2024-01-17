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
from utils import query, ETL
from utils.helpers import *

"""## Load api key"""

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

"""# Load data"""

data_path = "data/data.csv"
# Check if existing data
if not os.path.exists(data_path):
    Path('data').mkdir(parents=True, exist_ok=True)
    printy("Start search")
    # Extract first page
    dict_data_result = query.searchAPI(keys_data["data_key"])
    # Convert in DataFrame
    data = [ETL.dict2dataframe(dict_data_result)]

    data_size = dict_data_result["total"]
    iterations = data_size // 100 if data_size % 100 == 0 else (data_size // 100) + 1
    limit = 100
    for i in tqdm(range(1,iterations), desc="Extracting"):
        if data_size - (i*100) <= 100:
            limit = data_size - (i*100)
        dict_data_result = query.searchAPI(keys_data["data_key"], limit = limit, page = i)
        # Convert in DataFrame
        data.extend([ETL.dict2dataframe(dict_data_result)])
    printg("Finish search")
    # Save
    data = pd.concat(data)
    data.to_csv(data_path, sep = "|", index = False)

else:
    print("Exist data.csv")
    printy("Start load")
    data = pd.read_csv(data_path, sep = "|")
    printg("Finish load")


print("\t Total elements", len(data))

data = data.reset_index(drop= True)
data.tail()

"""# Transform data

## Simple transformation
"""

data["createdate"] = data["createdate"].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))

"""## Search Country"""

set(list(data["country"].values))

data[["contry_found","city_found"]] = data["country"].apply(lambda x: pd.Series(ETL.found_contry(x)))

# The output of this cell is finished because the implemented library contains an internal print that is executed at each interaction

# Errors
data[data["contry_found"] == "error"][["country","contry_found","city_found"]].tail()

# Same error?
data[data["contry_found"] == "error"]["country"].unique()

# review Nan data
data[~data["country"].isna()].head()

# Both the country and number information are empty, and it is evident that the records are repeated, so they will be eliminated
data = data[~data["country"].isna()]
data = data.reset_index(drop= True)
data.head()

# Set Winchester
for i in range(len(data)):
    if data.iloc[i]["country"] == "Winchester":
        data.loc.__setitem__((i, ('contry_found')), "United Kingdom")
        data.loc.__setitem__((i, ('city_found')), "Winchester")

data[data["country"] == "Winchester"]

"""## Regrex email"""

data["email"] = data["raw_email"].apply(lambda x: ETL.find_email(x))

# Errors
data[data["email"] == "error"][["raw_email","email"]].tail()

data.head(5)

"""## Fix phone number"""

data["contry_found"].unique()

# Having only two countries it is more convenient to use a dictionary to normalize than to implement some kind of external library
dict_normalize_phones = {
    "Ireland":"(+353)",
    "United Kingdom": "(+44)"
}

data["fix_phone"] = data[["phone","contry_found"]].apply(lambda x: ETL.normalize_phones(
    dict_normalize_phones = dict_normalize_phones,
    phone = x.iloc[0], country = x.iloc[1]), axis = 1)

data.head(3)

"""## Conact name"""

data["names"] = data[["firstname","lastname"]].apply(lambda x: f"{x.iloc[0]} {x.iloc[1]}", axis = 1)
data.head(4)

"""# Duplicates Management"""

#Review empty data
data.isna().sum()

"""## Review uniques email"""

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

data_postprocessing["industry"].unique()

#Review empty data
data_postprocessing.isna().sum()

# Others empty
data_postprocessing[data_postprocessing["firstname"].isna()]

# This record is deleted because we do not have complete information, although it can be assumed that his name is tyson and his last name is newman and his email address, it is decided not to display any information
data_postprocessing = data_postprocessing[~data_postprocessing["firstname"].isna()].reset_index(drop=True)

# Duplicate
len(data_postprocessing) == len(data_postprocessing["hs_object_id"].unique())

data_postprocessing.to_csv("data/data_processing.csv", sep="|",index=False)

"""# Saving data"""

data_postprocessing = pd.read_csv("data/data_processing.csv", sep="|")

data_postprocessing['city_found'] = data_postprocessing['city_found'].fillna('')
data_postprocessing["hs_object_id"] = data_postprocessing["hs_object_id"].astype(int)

data_postprocessing.columns

data_send = data_postprocessing[
    [
        "firstname", "lastname","email", "fix_phone", "contry_found", "city_found",
        "technical_test___create_date", "industry", "hs_object_id"
    ]
].copy()

data_send.head()

data_send.columns = [
    "firstname", "lastname","email", "phone", "country", "city",
    "original_create_date", "original_industry", "temporary_id"
]

data_send["response_save"] = [query.sendAPI(keys_data["account_key"], input_data = data_send.iloc[i]) \
                              for i in tqdm(range(len(data_send)))]

data_send["response_save"].value_counts()

