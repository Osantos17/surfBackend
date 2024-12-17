import requests
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('config.env')

def fetch_surf(lat, lng):
    api_key = os.getenv('API_KEY')
    
    if not api_key:
        print('Error: API key not found')
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
            weather_data = data['data'][0]['weather']  # Assuming there's at least one entry in the data
            insert_surf_data(lat, lng, weather_data)
        except KeyError as e:
            print(f"KeyError: {str(e)} - Surf data not found in the API response.")
    else:
        print(f"Error fetching data: {response.status_code}")

def insert_surf_data(lat, lng, weather_data):
    # Using environment variables for the database connection (recommended for Heroku and local dev)
    dbname = os.getenv('DB_NAME', 'surf_forecast')
    user = os.getenv('DB_USER', 'orlandosantos')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port
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

        for surf_data in weather_data:
            try:
                time = surf_data.get('time')
                swellHeight_ft = surf_data.get('swellHeight_ft')
                swellDir = surf_data.get('swellDir')
                swellDir16Point = surf_data.get('swellDir16Point')
                windspeedMiles = surf_data.get('windspeedMiles')
                winddirDegree = surf_data.get('winddirDegree')
                tempF = surf_data.get('tempF')  # If you need temperature as well
                weatherDesc = surf_data.get('weatherDesc')[0]['value'] if 'weatherDesc' in surf_data else None
                sunrise = surf_data.get('sunrise')  # Example if you need sunrise time
                sunset = surf_data.get('sunset')  # Example if you need sunset time

                # Use INSERT ... ON CONFLICT for upsert behavior
                cursor.execute(
                    '''INSERT INTO surf_data (
                        location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree, 
                        tempF, weatherDesc, sunrise, sunset
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_id, time) DO UPDATE SET
                        swellHeight_ft = EXCLUDED.swellHeight_ft,
                        swellDir = EXCLUDED.swellDir,
                        swellDir16Point = EXCLUDED.swellDir16Point,
                        windspeedMiles = EXCLUDED.windspeedMiles,
                        winddirDegree = EXCLUDED.winddirDegree,
                        tempF = EXCLUDED.tempF,
                        weatherDesc = EXCLUDED.weatherDesc,
                        sunrise = EXCLUDED.sunrise,
                        sunset = EXCLUDED.sunset''',
                    (location_id, time, swellHeight_ft, swellDir, swellDir16Point, windspeedMiles, winddirDegree,
                     tempF, weatherDesc, sunrise, sunset)
                )
                print(f"Inserted or updated surf data for location_id: {location_id} at time: {time}")

            except KeyError as e:
                print(f"KeyError for data entry: {e} - Missing expected field in API response")

    else:
        print(f"Location not found for latitude: {lat}, longitude: {lng}")

    conn.commit()
    cursor.close()
    conn.close()

# Example usage
fetch_surf(37.488897, -122.466919)
