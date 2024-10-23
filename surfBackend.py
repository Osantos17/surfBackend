import requests
import os
from dotenv import load_dotenv

load_dotenv('config.env')

def fetch_surf(lat, lng):
    api_key = os.getenv('API_KEY')
    
    if not api_key:
        print('Error: API Disconnected')
        return
        
    base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
    params = {
        'key': api_key,
        'format': 'json',  # Change to 'json' since you want JSON output
        'q': f'{lat},{lng}',  # Dynamic Latitude, Longitude
        'tide': 'yes',
    }

    # GET request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        # Print the JSON response data
        print(response.json())  # You can change this to save or filter the data
    else:
        print(f"Error: {response.status_code}")
        
fetch_surf(37.488897, -122.466919)
