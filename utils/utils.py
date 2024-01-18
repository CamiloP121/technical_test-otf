import requests
from models.helpers import *

def check_api_status(api_url, api_key) -> bool:
    """
    Checks the status of an API using an API key.
    ----------------------------------------------------------------
    Parameters:
    - api_url (str): The URL of the API you want to verify.
    - api_key (str): The API key for authentication.
    """
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    try:
        response = requests.post(api_url, headers=headers, json={})
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        printr("Error status")
        print(e)
        return False