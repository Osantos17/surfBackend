import psycopg2
import os
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import logging

load_dotenv('config.env')


def get_db_connection():
    # Always use local settings for the Postico database
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )
    return conn


def fetch_historical_tide_data(lat: float, lng: float) -> dict:
    """Fetch historical tide data from the API for the previous day."""
    print(f"Fetching tide data for latitude {lat} and longitude {lng}...")  # Add this line for clarity.
    
    # Get the API key from the environment variable
    api_key = os.getenv('API_KEY')
    if not api_key:
        print("Error: API key not found in environment.")
        return None
    
    base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
    
    # Get yesterday's date in the required format
    previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
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
    """Convert time from 12-hour format to 24-hour format."""
    try:
        # Parse the time string to a datetime object
        time_obj = datetime.strptime(time_str, '%I:%M %p')  # 12-hour format (e.g., 2:30 PM)
        # Convert to 24-hour format
        return time_obj.strftime('%H:%M')  # 24-hour format (e.g., 14:30)
    except ValueError:
        # If the time format is incorrect, return the original string
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
    """Fetch and update tide data for all locations in the locations table."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Fetch all locations
                cursor.execute('SELECT id, latitude, longitude FROM locations')
                locations = cursor.fetchall()

                # Iterate through all locations and update tide data
                for location in locations:
                    location_id, lat, lng = location
                    move_last_tide_to_boundary(location_id, lat, lng)
                    logging.info(f"Tide data updated for location ID {location_id}")

                conn.commit()
                logging.info("Tide data update complete for all locations.")
                
    except Exception as e:
        logging.error(f"Error updating tide data for all locations: {e}")
        
        cursor.execute('SELECT COUNT(*) FROM locations')
        print(cursor.fetchone())


# Run the update for all locations
update_tide_data_for_all_locations()

