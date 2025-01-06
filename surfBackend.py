import requests
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from config.env (only for local testing)
if os.getenv('ENV') != 'production':
    load_dotenv('config.env')


def get_db_connection():
    DATABASE_URL = os.getenv('DATABASE_URL')  # For Heroku
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'surf_forecast'),
            user=os.getenv('DB_USER', 'your_local_user'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )


def fetch_surf(lat, lng, location_id):
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
                insert_surf_data(location_id, weather_data)
            else:
                print("Warning: 'weather' data not found in the API response.")
        except KeyError as e:
            print(f"KeyError: {str(e)} - Surf data not found in the API response.")



def insert_surf_data(location_id, weather_data):
    dbname = os.getenv('DB_NAME', 'surf_forecast')
    user = os.getenv('DB_USER', 'orlandosantos')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        cursor.execute("DELETE FROM surf_data WHERE location_id = %s", (location_id,))


        for surf_data in weather_data:
            date = surf_data.get('date')
            date = datetime.strptime(date, '%Y-%m-%d').date()  # Convert to datetime.date object
            sunrise = surf_data['astronomy'][0].get('sunrise')
            sunset = surf_data['astronomy'][0].get('sunset')

            if sunrise:
                sunrise = datetime.strptime(sunrise, '%I:%M %p').time()  # Convert to time
            else:
                sunrise = None  # Handle missing sunrise

            if sunset:
                sunset = datetime.strptime(sunset, '%I:%M %p').time()  # Convert to time
            else:
                sunset = None  # Handle missing sunset

            for hourly_data in surf_data.get('hourly', []):
                time = hourly_data.get('time')
                temp_f = hourly_data.get('tempF')
                wind_speed = hourly_data.get('windspeedMiles')
                wind_dir_degree = hourly_data.get('winddirDegree')
                wind_dir_16pt = hourly_data.get('winddir16Point')  # Correct this part
                weather_desc = hourly_data['weatherDesc'][0].get('value')
                swell_height_ft = hourly_data.get('swellHeight_ft')
                swell_dir = hourly_data.get('swellDir')
                swell_period = hourly_data.get('swellPeriod_secs')
                water_temp_f = hourly_data.get('waterTemp_F')
                swell_dir_16pt = hourly_data.get('swellDir16Point')  # Correct this part
                print("swellDir16Point:", swell_dir_16pt)

            
                # Debugging output for the values
                print("Inserting values:", (
                    location_id, date, time, temp_f, wind_speed, wind_dir_degree,
                    wind_dir_16pt, weather_desc, swell_height_ft, swell_dir,
                    swell_period, water_temp_f, sunrise, sunset, swell_dir_16pt
                ))

                values = (
                    location_id, date, time, temp_f, wind_speed, wind_dir_degree,
                    wind_dir_16pt, weather_desc, swell_height_ft, swell_dir,
                    swell_period, water_temp_f, sunrise, sunset, swell_dir_16pt
                )
            
                # Debugging statement to check SQL query and values
                print(f"Inserting values into the database: {values}")
                print(f"swellDir16Point: {swell_dir_16pt}")

                cursor.execute(
                    '''INSERT INTO surf_data (
                        location_id, date, time, tempf, windspeedmiles, winddirdegree,
                        winddir16point, weatherdesc, swellheight_ft, swelldir,
                        swellperiod_secs, watertemp_f, sunrise, sunset, swelldir16point
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                    values
                )


        conn.commit()

    except Exception as e:
        print(f"Error inserting surf data: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def process_all_locations():
    DATABASE_URL = os.getenv('DATABASE_URL')  # Use Heroku's DATABASE_URL

    conn = None
    cursor = None  # Initialize cursor to avoid UnboundLocalError
    
    try:
        if DATABASE_URL:
            conn = psycopg2.connect(DATABASE_URL, sslmode='require')  # Heroku connection
        else:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME', 'surf_forecast'),
                user=os.getenv('DB_USER', 'orlandosantos'),
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432')
            )
        
        cursor = conn.cursor()
        cursor.execute('SELECT id, latitude, longitude FROM locations')
        locations = cursor.fetchall()

        for location in locations:
            location_id, lat, lng = location
            fetch_surf(lat, lng, location_id)

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Run for all locations
process_all_locations()
