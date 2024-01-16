import requests
from utils.helpers import *

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