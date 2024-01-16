import pandas as pd

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