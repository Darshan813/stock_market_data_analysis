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
    except Exception as e:
        print("Calling exception")
        print("Something went wrong: ", e)

    if __name__ == "__main__":
        print("Hello")

    return df