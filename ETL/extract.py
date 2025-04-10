
from dotenv import load_dotenv
import pandas as pd
from fredapi import Fred
import os

class Extractor:
    def __init__ (self, api_key:str):
        if not api_key:
            raise ValueError ("FRED_API_KEY not found in environment")
        self.fred = Fred(api_key=api_key)
        


    def extract(self, series_id: str):
        try:
            data = self.fred.get_series(series_id)
            return data
        except Exception as e:
            raise e