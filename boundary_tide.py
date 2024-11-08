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
    print("Fetching tide data...")  # Add this line to see if the function is called.
    
    # Get the API key from the environment variable
    api_key = os.getenv('API_KEY')
    if not api_key:
        print("Error: API key not found in environment.")
        return None
    
    base_url = "http://api.worldweatheronline.com/premium/v1/past-marine.ashx"
    
    
    # Get yesterday's date in the required format
    previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Previous day: {previous_day}")

    
    params = {
        'key': api_key,
        'format': 'json',
        'q': f'{lat},{lng}',
        'date': previous_day,
        'tide': 'yes',
    }

    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        print("API Response:")
        print(response.json())  # Print the full response for debugging
        return response.json()
    else:
        print(f"Error fetching historical tide data: {response.status_code}")
        return None

def move_last_tide_to_boundary(location_id: int, lat: float, lng: float) -> None:
    """Fetch the last tide entry for the previous day and insert it into boundary_tide_data."""
    try:
        tide_data = fetch_historical_tide_data(lat, lng)
        if tide_data:
            if 'data' in tide_data and 'weather' in tide_data['data']:
                weather_data = tide_data['data']['weather'][0]
                
                # Log the weather data
                print(f"Weather data: {weather_data}")
                
                # If there are tide entries, get the last one
                if 'tides' in weather_data and weather_data['tides']:
                    last_tide_entry = weather_data['tides'][0]['tide_data'][-1]  # Last tide event of the day
                    
                    # Log the last tide entry
                    print(f"Last tide entry: {last_tide_entry}")
                    
                    # Prepare data for insertion
                    tide_time = last_tide_entry['tideTime']
                    tide_height_mt = last_tide_entry['tideHeight_mt']
                    tide_type = last_tide_entry['tide_type']
                    tide_date = datetime.strptime(weather_data['date'], '%Y-%m-%d').date()
                    
                    # Log the prepared data before inserting it
                    print(f"Inserting data: location_id={location_id}, tide_time={tide_time}, tide_height_mt={tide_height_mt}, tide_type={tide_type}, tide_date={tide_date}")
                    
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    
                    # Insert into boundary_tide_data
                    cursor.execute('''
                        INSERT INTO boundary_tide_data (
                            location_id, tide_time, tide_height_mt, tide_type, tide_date
                        ) VALUES (%s, %s, %s, %s, %s)
                    ''', (location_id, tide_time, tide_height_mt, tide_type, tide_date))
                    
                    conn.commit()
                    print("Last tide entry from previous day added to boundary_tide_data.")
                
            else:
                print("No weather data or tides found in the response.")
        else:
            print("No tide data returned from API.")
                
    except Exception as e:
        print(f"Error moving last tide entry to boundary_tide_data: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Example usage
location_id = 1  # Set the location ID
lat = 37.7749    # Set the latitude of the location
lng = -122.4194  # Set the longitude of the location
move_last_tide_to_boundary(location_id, lat, lng)
