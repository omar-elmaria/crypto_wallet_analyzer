# Import packages
import re
from typing import Any

## Helper functions
def helper_normalize_millions_and_thousands_in_vol_liq_mcap(value: str) -> float:
    """
    A function to normalize the volume, liquidity, and market capitalization values in millions and thousands.
    """
    if value.find("T") != -1:
        value = float(re.sub(pattern="T", repl="", string=value)) * 1000000
    elif value.find("B") != -1:
        value = float(re.sub(pattern="B", repl="", string=value)) * 1000
    elif value.find("M") != -1:
        value = float(re.sub(pattern="M", repl="", string=value))
    elif value.find("K") != -1:
        value = float(re.sub(pattern="K", repl="", string=value)) / 1000
    else:
        value = float(value)
    
    return value

def helper_normalize_millions_and_billions_in_pct_gains(value: str) -> float:
    """
    A function to normalize the percentage gains in the last 5 minutes, 1 hour, 6 hours, and 24 hours.
    """
    if value.find("B") != -1:
        value = float(re.sub(pattern="%|B|,", repl="", string=value)) * pow(10, 9)
    elif value.find("M") != -1:
        value = float(re.sub(pattern="%|M|,", repl="", string=value)) * pow(10, 6)
    elif value.find("K") != -1:
        value = float(re.sub(pattern="%|K|,", repl="", string=value)) * pow(10, 3)
    else:
        value = float(re.sub(pattern="%|,", repl="", string=value))
    
    return value

def helper_treat_none_before_data_type_change(value: str, data_type: Any):
    """
    A function to treat None values before changing the data type of the values.
    """
    if value is not None:
        if data_type == "int":
            value = int(value)
        elif data_type == "float":
            value = float(value)
    return value