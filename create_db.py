import psycopg2
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv('config.env')

def get_db_connection():
    DATABASE_URL = os.environ.get('DATABASE_URL')

    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        # For local testing or fallback, use local settings (only for local development)
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="your_local_user",  # Adjust accordingly for local testing
            host="localhost",
            port="5432"
        )
    
    return conn


def create_db() -> None:
    """Create the database and necessary tables."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id SERIAL PRIMARY KEY,
                location_name VARCHAR(100) NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                preferred_wind_dir_min INTEGER,
                preferred_wind_dir_max INTEGER,
                preferred_swell_dir_min INTEGER,
                preferred_swell_dir_max INTEGER,
                bad_swell_dir_min INTEGER,
                bad_swell_dir_max INTEGER,
                wavecalc NUMERIC,
                region VARCHAR(50)
            )
        ''')
        

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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_time VARCHAR(10),
                tide_height_mt FLOAT,
                tide_type VARCHAR(10),
                tide_date DATE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS graph_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_date DATE,
                tide_height_mt DOUBLE PRECISION,
                tide_time TIME WITHOUT TIME ZONE,
                tide_time_numeric INTEGER,
                tide_type VARCHAR(50)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS graph_points (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                graph_date DATE NOT NULL,
                graph_time TIME WITHOUT TIME ZONE NOT NULL,
                tide_height NUMERIC,
                tide_type VARCHAR(20)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS boundary_tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_height_mt DOUBLE PRECISION,
                tide_date DATE,
                tide_time VARCHAR(20),
                tide_type VARCHAR(20)
            )
        ''')

        conn.commit()
        print("Database and tables created successfully!")

    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()
        
    
        
        
def add_columns_to_locations_table() -> None:
    """Add missing columns to the locations table."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Add new columns to the locations table
        cursor.execute('''
            ALTER TABLE locations
            ADD COLUMN IF NOT EXISTS preferred_wind_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_wind_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_swell_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_swell_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS bad_swell_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS bad_swell_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS wavecalc VARCHAR(100),
            ADD COLUMN IF NOT EXISTS Region VARCHAR(50); 
        ''')

        conn.commit()
        print("Columns added successfully!")

    except Exception as e:
        print(f"Error adding columns: {e}")
    finally:
        cursor.close()
        conn.close()



# def move_last_tide_to_boundary(location_id: int) -> None:
#     """Move the last tide entry of the previous day from tide_data to boundary_tide_data."""
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Ensure location_id is passed or defined
#         cursor.execute('''
#             INSERT INTO boundary_tide_data (location_id, tide_time, tide_height_mt, tide_type, tide_date)
#             SELECT location_id, 
#                    CASE 
#                        WHEN tide_time = '12:00 AM' THEN '00:00'  -- Handle midnight as 00:00
#                        ELSE to_char(tide_time::TIME, 'HH24:MI')
#                    END AS tide_time_24hr,
#                    tide_height_mt, 
#                    tide_type, 
#                    tide_date
#             FROM tide_data
#             WHERE location_id = %s AND tide_date = CURRENT_DATE - INTERVAL '1 day'
#             ORDER BY tide_time DESC LIMIT 1
#             ON CONFLICT (location_id, tide_date, tide_time) DO NOTHING;
#         ''', (location_id,))

#         conn.commit()
#         print(f"Moved last tide entry to boundary_tide_data for location {location_id}.")

#     except Exception as e:
#         print(f"Error moving last tide entry for location {location_id}: {e}")
#     finally:
#         cursor.close()
#         conn.close()


# def fetch_surf_data(lat: float, lng: float) -> dict:
#     """Fetch surf data from the API."""
#     api_key = os.getenv('API_KEY')
#     if not api_key:
#         print("API key not found in environment variables.")
#         return None

#     base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
    
#     # Get today's date and the next two days
#     today = datetime.now().strftime('%Y-%m-%d')
#     next_day = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
#     day_after_next = (datetime.now() + timedelta(2)).strftime('%Y-%m-%d')

#     # Assuming the API supports multiple days by passing a list of dates (check API documentation)
#     params = {
#         'key': api_key,
#         'format': 'json',
#         'q': f'{lat},{lng}',
#         'tide': 'yes',
#         'date': f'{today},{next_day},{day_after_next}'  # Fetch data for today, next day, and the day after next
#     }

#     response = requests.get(base_url, params=params)

#     if response.status_code == 200:
#         return response.json()
#     else:
#         print(f"Error fetching data: {response.status_code}")
#         return None

    
# def insert_surf_data(location_id: int, data: dict) -> None:
#     """Insert surf and tide data into the database."""
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Clear existing data in the tables before inserting new data
#         cursor.execute('DELETE FROM surf_data WHERE location_id = %s', (location_id,))
#         cursor.execute('DELETE FROM tide_data WHERE location_id = %s', (location_id,))
#         print(f"Deleted existing surf data and tide data for location ID: {location_id}")

#         insert_query = '''
#             INSERT INTO surf_data (
#                 location_id, date, sunrise, sunset, time, tempF,
#                 windspeedMiles, winddirDegree, winddir16Point,
#                 weatherDesc, swellHeight_ft, swellDir, swellDir16Point,
#                 swellPeriod_secs, waterTemp_F
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         '''
        
#         insert_tide_query = '''
#             INSERT INTO tide_data (
#                 location_id, tide_time, tide_height_mt, tide_type, tide_date
#             )
#             VALUES (%s, %s, %s, %s, %s)
#         '''

#         for weather_data in data['data']['weather']:
#             astronomy_data = weather_data['astronomy'][0]
#             selected_hours = ['300', '600', '900', '1200', '1500', '1800', '2100'] 

#             formatted_date = weather_data['date']

#             for hourly_data in weather_data['hourly']:
#                 if hourly_data['time'] in selected_hours:
#                     record = (
#                         location_id,
#                         formatted_date,
#                         astronomy_data['sunrise'],
#                         astronomy_data['sunset'],
#                         hourly_data['time'],  # Now directly using time in 24-hour format
#                         hourly_data['tempF'],
#                         hourly_data['windspeedMiles'],
#                         hourly_data['winddirDegree'],
#                         hourly_data['winddir16Point'],
#                         hourly_data['weatherDesc'][0]['value'],
#                         hourly_data['swellHeight_ft'],
#                         hourly_data['swellDir'],
#                         hourly_data['swellDir16Point'],
#                         hourly_data['swellPeriod_secs'],
#                         hourly_data['waterTemp_F']
#                     )

#                     cursor.execute(insert_query, record)

#             if 'tides' in weather_data:
#                 for tide_event in weather_data['tides'][0]['tide_data']:
#                     tide_time_24hr = datetime.strptime(tide_event['tideTime'], '%I:%M %p').strftime('%H:%M')  # Convert time to 24-hour format
#                     tide_record = (
#                         location_id,
#                         tide_time_24hr,
#                         tide_event['tideHeight_mt'],
#                         tide_event['tide_type'],
#                         datetime.fromisoformat(tide_event['tideDateTime']).date()
#                     )
#                     cursor.execute(insert_tide_query, tide_record)

#         conn.commit()
#         print("Surf data inserted successfully!")

#     except Exception as e:
#         print(f"Error inserting surf data: {e}")
#     finally:
#         cursor.close()
#         conn.close()

if __name__ == "__main__":
    create_db()  
    add_columns_to_locations_table() 

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT id, latitude, longitude FROM locations')
        locations = cursor.fetchall()

        for location in locations:
            location_id, lat, lng = location
            # surf_data = fetch_surf_data(lat, lng)
            # if surf_data:
            #     insert_surf_data(location_id, surf_data)

            # move_last_tide_to_boundary(location_id)

    except Exception as e:
        print(f"Error connecting to database: {e}")
    finally:
        cursor.close()
        conn.close()
