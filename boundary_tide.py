import psycopg2
import os
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import logging
from urllib.parse import urlparse
import pytz


if os.getenv('ENV') != 'production':
    load_dotenv('config.env')

def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        url_parts = urlparse(db_url)
        return psycopg2.connect(
            database=url_parts.path[1:],
            user=url_parts.username,
            password=url_parts.password,
            host=url_parts.hostname,
            port=url_parts.port
        )
    else:
        raise Exception("DATABASE_URL not set in production")


def fetch_historical_tide_data(lat: float, lng: float) -> dict:
    print(f"Fetching tide data for latitude {lat} and longitude {lng}...")

    api_key = os.getenv('API_KEY')
    if not api_key:
        print("Error: API key not found in environment.")
        return None
    
    base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
    
    # Set to local timezone
    local_timezone = pytz.timezone('America/Los_Angeles')  # Adjust to your local timezone
    now_local = datetime.now(local_timezone)
    previous_day = (now_local - timedelta(days=1)).strftime('%Y-%m-%d')
    
    params = {
        'key': api_key,
        'format': 'json',
        'q': f'{lat},{lng}',
        'date': previous_day,
        'tide': 'yes',
    }

    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching historical tide data: {response.status_code}")
        return None


def convert_to_24hr_format(time_str: str) -> str:
    try:
        time_obj = datetime.strptime(time_str, '%I:%M %p')
        return time_obj.strftime('%H:%M')
    except ValueError:
        return time_str


def move_last_tide_to_boundary(location_id: int, lat: float, lng: float) -> None:
    try:
        tide_data = fetch_historical_tide_data(lat, lng)
        
        if tide_data and 'data' in tide_data and 'weather' in tide_data['data']:
            weather_data = tide_data['data']['weather'][0]
            
            if 'tides' in weather_data and weather_data['tides']:
                last_tide_entry = weather_data['tides'][0]['tide_data'][-1]
                
                tide_time = last_tide_entry['tideTime']
                tide_time_24hr = convert_to_24hr_format(tide_time)
                tide_height_mt = last_tide_entry['tideHeight_mt']
                tide_type = last_tide_entry['tide_type']
                tide_date = datetime.strptime(weather_data['date'], '%Y-%m-%d').date()
                
                print(f"Fetched data: {tide_time_24hr}, {tide_height_mt}, {tide_type} for {tide_date}")
                
                conn = get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    DELETE FROM boundary_tide_data
                    WHERE location_id = %s
                ''', (location_id,))
                
                cursor.execute('''
                    INSERT INTO boundary_tide_data (
                        location_id, tide_time, tide_height_mt, tide_type, tide_date
                    ) VALUES (%s, %s, %s, %s, %s)
                ''', (location_id, tide_time_24hr, tide_height_mt, tide_type, tide_date))
                
                print(f"Inserting tide data for location {location_id} at {tide_time_24hr}")
                
                conn.commit()
                print(f"Tide data updated for location {location_id}.")
            else:
                print(f"No tide data found for location {location_id}.")
                
        else:
            print(f"No valid weather data returned for location {location_id}.")
                
    except Exception as e:
        print(f"Error moving tide entry to boundary_tide_data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def update_tide_data_for_all_locations():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT id, latitude, longitude FROM locations')
                locations = cursor.fetchall()

                for location in locations:
                    location_id, lat, lng = location
                    move_last_tide_to_boundary(location_id, lat, lng)
                    logging.info(f"Tide data updated for location ID {location_id}")

                conn.commit()
                logging.info("Tide data update complete for all locations.")
                
    except Exception as e:
        logging.error(f"Error updating tide data for all locations: {e}")


# Run the update for all locations
update_tide_data_for_all_locations()
