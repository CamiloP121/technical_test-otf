import requests
import json
from models.helpers import *
import pandas as pd

def searchAPI(api_key:str, page:str = 0, limit:int = 100) -> dict:
    """
    Search through the API for a specific data collection.
    Database: contacts
    -----------------------------------------------------------------
    Args:
    - api_key (str): API key necessary for using the request
    - page (str, optional): Page number for paginated results (default is 0)
    - limit (int, optional): Number of results per page (default is 100)
    Returns:
    - response (dict): results of the search request
    """

    assert api_key != "", "Empty API key"

    # Input parameters
    input = {
        "filters": [
        # Filter: allowed_to_collect == True
            {
            "propertyName": "allowed_to_collect",
            "operator": "EQ",
            "value": "true"
            }
        ],
        "limit": limit,
        "after": f"{page}", 
        # Data extract
        "properties": ["firstname","lastname",
                       "raw_email", "country", "phone", 
                       "technical_test___create_date", 
                       "industry", "address", "address", 
                       "hs_object_id"]
    }
    
    # Other parameters
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }

    # Search
    
    response = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/search", 
                                 json=input, headers=headers).json()
    
    if "status" in response:
        printr("Error in:")
        print(response["message"])
        raise Exception("Error consult API")
    
    return response


def sendAPI(api_key:str, input_data:pd.DataFrame) -> dict:
    """
    Search through the API for a specific data collection.
    Database: contacts
    -----------------------------------------------------------------
    Args:
    - api_key (str): API key necessary for using the request
    - input_data (DataFrame): Row as send to create a new contact
    Returns:
    - response (dict): results of the search request
    """

    assert api_key != "", "Empty API key"

    # Input parameters
    json_data_send = {
        "inputs": [
            {
                "properties": {
                     "firstname": input_data["firstname"],
            "lastname": input_data["lastname"],
            "email": input_data["email"],
            "phone": input_data["phone"],
            "country": input_data["country"],
            "city": input_data["city"],
            "original_create_date": input_data["original_create_date"],
            "original_industry": input_data["original_industry"],
            "temporary_id": int(input_data["temporary_id"])
                }
            }
        ]
    }
    
    # Other parameters
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }

    # Search
    response = requests.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/create", 
                                 json=json_data_send, headers=headers).json()
    
    if  response["status"] == "error":
        printr("Error in:")
        print(response)
        print("\n", input)
    
    return response["status"]