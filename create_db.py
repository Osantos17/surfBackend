import psycopg2
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('config.env')

def get_db_connection():
    """Establish a database connection."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

def create_db() -> None:
    """Create the database and necessary tables."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create locations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id SERIAL PRIMARY KEY,
                location_name VARCHAR(100) NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL
            )
        ''')

        # Create surf_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS surf_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                date VARCHAR(20) NOT NULL,
                sunrise VARCHAR(10),
                sunset VARCHAR(10),
                time VARCHAR(10),
                tempF FLOAT,
                windspeedMiles FLOAT,
                winddirDegree INT,
                winddir16Point VARCHAR(10),
                weatherDesc VARCHAR(100),
                swellHeight_ft FLOAT,
                swellDir INT,
                swellDir16Point VARCHAR(10),
                swellPeriod_secs FLOAT,
                waterTemp_F FLOAT
            )
        ''')
        
        # Create tide_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_time VARCHAR(10),
                tide_height_mt FLOAT,
                tide_type VARCHAR(10),
                tide_date DATE  -- Change TIMESTAMP to DATE
            )
        ''')


        conn.commit()
        print("Database and tables created successfully!")

    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()

def fetch_surf_data(lat: float, lng: float) -> dict:
    """Fetch surf data from the API."""
    api_key = os.getenv('API_KEY')
    base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
    params = {
        'key': api_key,
        'format': 'json',
        'q': f'{lat},{lng}',
        'tide': 'yes',
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def convert_to_12hr_format(time_str: str) -> str:
    """Convert time from 24-hour to 12-hour format."""
    hour = int(time_str) // 100
    am_pm = "AM" if hour < 12 else "PM"
    hour = hour if hour <= 12 else hour - 12
    hour = 12 if hour == 0 else hour  # Handle midnight and noon
    return f"{hour}{am_pm}"

def insert_surf_data(location_id: int, data: dict) -> None:
    """Insert surf and tide data into the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Clear existing data in the tables before inserting new data
        cursor.execute('DELETE FROM surf_data WHERE location_id = %s', (location_id,))
        cursor.execute('DELETE FROM tide_data WHERE location_id = %s', (location_id,))
        print(f"Deleted existing surf data and tide data for location ID: {location_id}")

        insert_query = '''
            INSERT INTO surf_data (
                location_id, date, sunrise, sunset, time, tempF,
                windspeedMiles, winddirDegree, winddir16Point,
                weatherDesc, swellHeight_ft, swellDir, swellDir16Point,
                swellPeriod_secs, waterTemp_F
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        insert_tide_query = '''
            INSERT INTO tide_data (
                location_id, tide_time, tide_height_mt, tide_type, tide_date
            )
            VALUES (%s, %s, %s, %s, %s)
        '''

        for weather_data in data['data']['weather']:
            astronomy_data = weather_data['astronomy'][0]
            selected_hours = ['300', '600', '900', '1200', '1500', '1800', '2100'] 

            # Use the full date in YYYY-MM-DD format
            formatted_date = weather_data['date']  # This should already be in the correct format (YYYY-MM-DD)

            for hourly_data in weather_data['hourly']:
                if hourly_data['time'] in selected_hours:
                    time_12hr = convert_to_12hr_format(hourly_data['time'])
                    record = (
                        location_id,
                        formatted_date,  # Use formatted_date here instead of month_day
                        astronomy_data['sunrise'],
                        astronomy_data['sunset'],
                        time_12hr,
                        hourly_data['tempF'],
                        hourly_data['windspeedMiles'],
                        hourly_data['winddirDegree'],
                        hourly_data['winddir16Point'],
                        hourly_data['weatherDesc'][0]['value'],
                        hourly_data['swellHeight_ft'],
                        hourly_data['swellDir'],
                        hourly_data['swellDir16Point'],
                        hourly_data['swellPeriod_secs'],
                        hourly_data['waterTemp_F']
                    )

                    cursor.execute(insert_query, record)


            if 'tides' in weather_data:
                for tide_event in weather_data['tides'][0]['tide_data']:
                    tide_record = (
                        location_id,
                        tide_event['tideTime'],
                        tide_event['tideHeight_mt'],
                        tide_event['tide_type'],
                        datetime.fromisoformat(tide_event['tideDateTime']).date()  # Use .date() to extract only the date
                    )
                    cursor.execute(insert_tide_query, tide_record)


        conn.commit()
        print("Surf data inserted successfully!")

    except Exception as e:
        print(f"Error inserting surf data: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_db()

    # Connect to the database to retrieve existing locations
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id, latitude, longitude FROM locations')
        locations = cursor.fetchall()

        for location in locations:
            location_id, lat, lng = location
            surf_data = fetch_surf_data(lat, lng)
            if surf_data:
                insert_surf_data(location_id, surf_data)

    except Exception as e:
        print(f"Error connecting to database: {e}")
    finally:
        cursor.close()
        conn.close()