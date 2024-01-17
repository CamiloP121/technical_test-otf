import pandas as pd
import pycountry
import re
from utils.helpers import *

def dict2dataframe(dict) -> pd.DataFrame:
    """
    Convert a dictionary containing result data into a Pandas DataFrame.
    ------------------------------------------------------------------
    Parameters:
    - dict (dict): A dictionary containing result data, including a "result" key with a non-empty list
                   and each element in the list having a "properties" key.
    Returns:
    - pd.DataFrame: A Pandas DataFrame containing the "properties" data from the input dictionary.
    """

    assert "results" in dict.keys(), "The dictionary is missing the 'result' key."
    assert dict["results"], "The 'result' list in the dictionary is empty."
    assert "properties" in dict["results"][0], "Each element in the 'result' list should have a 'properties' key."

    temporal_vector_data = [
        dict["results"][i]["properties"] for i in range(len(dict["results"]))
    ]

    return pd.DataFrame(temporal_vector_data)

def found_contry(country: str):
    """
    Search for a country and/or city using a fuzzy search with the pycountry library.
    ------------------------------------------------------------------
    Parameters:
    - country (str): The name of the country to search for
    Returns:
    - tuple: A tuple containing the found country name and city
    """
    try:
        response = pycountry.countries.search_fuzzy(country)
        country_search = response[0].name
    except Exception as e:
        printr(e)
        country_search = "error" 

    city = ""

    if country_search != country:
        city = country
    
    return country_search, city


def find_email(txt: str) -> str:
    """
    Extracts an email address from the given text using a regular expression.
    -------------------------------------------------------------------
    Arg:
    - txt (str): The input text from which to extract the email address
    Returns:
    - str: The extracted email address, or "Error" if no email address is found
    """

    regular_expression = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    try:
        # Use re.findall to find all email addresses in the text, and take the first one
        emails_found = re.findall(regular_expression, txt)[0]
    except Exception as e:
        print("Error:", e)
        emails_found = "Error"
    return emails_found

def normalize_phones(dict_normalize_phones:dict, phone:str, country:str) -> str:
    """
    Normalize a phone number based on a dictionary of country-specific standardizations.
    ---------------------------------------------------------------
    Parameters:
    - dict_normalize_phones (dict): A dictionary containing country-specific phone number standardizations
    - phone (str): The original phone number to be normalized
    - country (str): The country associated with the phone number
    Returns:
    - str: The normalized phone number
    """
    
    assert phone != "", "Empty phone"
    assert country in dict_normalize_phones.keys(), f"{country} is not in the dictionary of standardizations"
    
    if phone[0] == "0": phone = phone[1:]
    tmp_phone = "".join(phone.split("-"))
    return f"{dict_normalize_phones[country]} {tmp_phone}"