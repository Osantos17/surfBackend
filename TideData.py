import requests
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from urllib.parse import urlparse

if os.getenv('ENV') != 'production':
    load_dotenv('config.env')

def fetch_tide(lat, lng, location_id):
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

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        print("API Response:", data)

        try:
            if 'data' in data and data['data'].get('weather'):
                weather_data = data['data']['weather']
                insert_tide_data(location_id, weather_data)
            else:
                print("Warning: 'weather' data not found in the API response.")
        except KeyError as e:
            print(f"KeyError: {str(e)} - Tide data not found in the API response.")

def insert_tide_data(location_id, weather_data):
    dbname = os.getenv('DB_NAME', 'surf_forecast')
    user = os.getenv('DB_USER', 'orlandosantos')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Delete existing tide data for the location
        cursor.execute("DELETE FROM tide_data WHERE location_id = %s", (location_id,))

        for weather in weather_data:
            if 'tides' in weather:
                tide_data = weather['tides'][0]['tide_data']
                tide_date = weather.get('date')
                tide_date = datetime.strptime(tide_date, '%Y-%m-%d').date()

                for tide_event in tide_data:
                    tide_time = datetime.strptime(tide_event['tideTime'], '%I:%M %p').time()
                    tide_height = tide_event.get('tideHeight_mt')
                    tide_type = tide_event.get('tide_type')
                    
                    print("Inserting tide values:", (location_id, tide_time, tide_height, tide_type, tide_date))

                    cursor.execute(
                        '''INSERT INTO tide_data (
                            location_id, tide_time, tide_height_mt, tide_type, tide_date
                        ) VALUES (%s, %s, %s, %s, %s)''',
                        (location_id, tide_time, tide_height, tide_type, tide_date)
                    )

        conn.commit()

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Ensure cursor and conn exist before attempting to close them
        try:
            if cursor:
                cursor.close()
        except UnboundLocalError:
            pass
        
        try:
            if conn:
                conn.close()
        except UnboundLocalError:
            pass
        
def get_db_connection():
    # Check if the app is running on Heroku or locally
    if os.getenv('ENV') == 'production':
        # Use Heroku's DATABASE_URL for production
        db_url = os.getenv('DATABASE_URL')
        url_parts = urlparse(db_url)
        return psycopg2.connect(
            database=url_parts.path[1:],
            user=url_parts.username,
            password=url_parts.password,
            host=url_parts.hostname,
            port=url_parts.port
        )
    else:
        # Use local environment settings for local development
        dbname = os.getenv('DB_NAME', 'surf_forecast')
        user = os.getenv('DB_USER', 'orlandosantos')
        host = os.getenv('DB_HOST', 'localhost')
        port = os.getenv('DB_PORT', '5432')
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            port=port
        )

def process_all_locations():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id, latitude, longitude FROM locations')
        locations = cursor.fetchall()

        for location in locations:
            location_id, lat, lng = location
            fetch_tide(lat, lng, location_id)

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Ensure cursor and conn exist before attempting to close them
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Run for all locations
process_all_locations()
