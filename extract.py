import requests
import csv
import json
from transform import transform

def extract(url):
    try:
        response = requests.get(url)
        data = response.json()
        stock_name = data["Meta Data"]["2. Symbol"]
        df = transform(data, stock_name)
        return df
    except Exception as e:
        print("Something went wrong: ", e)
