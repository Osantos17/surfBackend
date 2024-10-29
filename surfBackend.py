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
        'format': 'json',  # Change to 'json' since you want JSON output
        'q': f'{lat},{lng}',  # Dynamic Latitude, Longitude
        'tide': 'yes',
    }

    # GET request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("API Response:", data)  # Print the entire response for debugging
        
        # Check if the surf data exists in the response
        try:
            # This assumes that the surf data is in the 'weather' list within the first item of 'data'
            surf_data_list = data['data'][0]['weather']  # Adjust based on actual API response structure
            
            # Call function to insert surf data into the database
            insert_surf_data(lat, lng, surf_data_list)
        except KeyError as e:
            print(f"KeyError: {str(e)} - Surf data not found in the API response.")
    else:
        print(f"Error: {response.status_code}")

def insert_surf_data(lat, lng, surf_data_list):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()

    # Get the location_id based on latitude and longitude (if needed)
    cursor.execute("SELECT id FROM locations WHERE latitude = %s AND longitude = %s", (lat, lng))
    location_id = cursor.fetchone()
    
    if location_id:
        location_id = location_id[0]

        # Clear existing surf data for this location
        cursor.execute('DELETE FROM surf_data WHERE location_id = %s', (location_id,))
        print(f"Deleted existing surf data for location_id: {location_id}")

        for surf_data in surf_data_list:
            # Extract relevant data from surf_data
            time = surf_data.get('time')  # Adjust this to match the API response
            swellHeight_ft = surf_data.get('swellHeight_ft')
            swellDir = surf_data.get('swellDir')
            swellDir16Point = surf_data.get('swellDir16Point')
            windspeedMiles = surf_data.get('windspeedMiles')
            winddirDegree = surf_data.get('winddirDegree')

            # Insert the surf data into the database
            cursor.execute(
                '''INSERT INTO surf_data (location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree)
                VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                (location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree)
            )
            print(f"Inserted surf data for location_id: {location_id} at time: {time}")

    conn.commit()
    cursor.close()
    conn.close()



# Example usage
fetch_surf(37.488897, -122.466919)
