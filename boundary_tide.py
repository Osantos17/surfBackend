import psycopg2
import os
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

load_dotenv('config.env')


def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

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
    """Fetch the last tide entry for the previous day and insert it into boundary_tide_data."""
    try:
        tide_data = fetch_historical_tide_data(lat, lng)
        if tide_data:
            if 'data' in tide_data and 'weather' in tide_data['data']:
                weather_data = tide_data['data']['weather'][0]
                
                # If there are tide entries, get the last one
                if 'tides' in weather_data and weather_data['tides']:
                    last_tide_entry = weather_data['tides'][0]['tide_data'][-1]  # Last tide event of the day
                    
                    # Prepare data for insertion
                    tide_time = last_tide_entry['tideTime']
                    tide_time_24hr = convert_to_24hr_format(tide_time)  # Convert to 24-hour format
                    tide_height_mt = last_tide_entry['tideHeight_mt']
                    tide_type = last_tide_entry['tide_type']
                    tide_date = datetime.strptime(weather_data['date'], '%Y-%m-%d').date()
                    
                    conn = get_db_connection()
                    cursor = conn.cursor()

                    # Delete all existing tide entries for the location and tide_date
                    cursor.execute('''
                        DELETE FROM boundary_tide_data
                        WHERE location_id = %s AND tide_date = %s
                    ''', (location_id, tide_date))
                    
                    # Insert the new tide data into boundary_tide_data
                    cursor.execute('''
                        INSERT INTO boundary_tide_data (
                            location_id, tide_time, tide_height_mt, tide_type, tide_date
                        ) VALUES (%s, %s, %s, %s, %s)
                    ''', (location_id, tide_time_24hr, tide_height_mt, tide_type, tide_date))
                    
                    conn.commit()
                    print(f"Tide data for location ID {location_id} replaced successfully.")
                
            else:
                print("No weather data or tides found in the response.")
        else:
            print("No tide data returned from API.")
                
    except Exception as e:
        print(f"Error moving tide entry to boundary_tide_data: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def update_tide_data_for_all_locations():
    """Fetch and update tide data for all locations in the locations table."""
    try:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch all locations (id, lat, lng) - update column name if necessary
        cursor.execute('SELECT id, latitude, longitude FROM locations')
        locations = cursor.fetchall()

        # Iterate through all locations and update tide data
        for location in locations:
            location_id, lat, lng = location
            move_last_tide_to_boundary(location_id, lat, lng)

        print("Tide data update complete for all locations.")
        
    except Exception as e:
        print(f"Error updating tide data for all locations: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Run the update for all locations
update_tide_data_for_all_locations()

