import requests
import os
import psycopg2
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
        'format': 'json',
        'q': f'{lat},{lng}',
        'tide': 'yes',
    }

    # GET request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("API Response:", data)
        
        # Check if the surf data exists in the response
        try:
            surf_data_list = data['data'][0]['weather']
            insert_surf_data(lat, lng, surf_data_list)
        except KeyError as e:
            print(f"KeyError: {str(e)} - Surf data not found in the API response.")
    else:
        print(f"Error: {response.status_code}")

def insert_surf_data(lat, lng, surf_data_list):
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()

    # Get the location_id based on latitude and longitude
    cursor.execute("SELECT id FROM locations WHERE latitude = %s AND longitude = %s", (lat, lng))
    location_id = cursor.fetchone()
    
    if location_id:
        location_id = location_id[0]

        # Clear existing surf data for this location (optional, if using upsert)
        cursor.execute('DELETE FROM surf_data WHERE location_id = %s', (location_id,))
        print(f"Deleted existing surf data for location_id: {location_id}")

        for surf_data in surf_data_list:
            time = surf_data.get('time')
            swellHeight_ft = surf_data.get('swellHeight_ft')
            swellDir = surf_data.get('swellDir')
            swellDir16Point = surf_data.get('swellDir16Point')
            windspeedMiles = surf_data.get('windspeedMiles')
            winddirDegree = surf_data.get('winddirDegree')

            # Use INSERT ... ON CONFLICT for upsert behavior
            cursor.execute(
                '''INSERT INTO surf_data (location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (location_id, time) DO UPDATE SET
                    swellHeight_ft = EXCLUDED.swellHeight_ft,
                    swellDir = EXCLUDED.swellDir,
                    swellDir16Point = EXCLUDED.swellDir16Point,
                    windspeedMiles = EXCLUDED.windspeedMiles,
                    winddirDegree = EXCLUDED.winddirDegree''',
                (location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree)
            )
            print(f"Inserted or updated surf data for location_id: {location_id} at time: {time}")

    conn.commit()
    cursor.close()
    conn.close()

# Example usage
fetch_surf(37.488897, -122.466919)
