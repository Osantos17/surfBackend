# import requests
# import os
# import psycopg2
# from dotenv import load_dotenv
# from datetime import datetime
# from urllib.parse import urlparse

# # Load environment variables from config.env (only for local testing)
# if os.getenv('ENV') != 'production':
#     load_dotenv('config.env')

# def get_db_connection():
#     DATABASE_URL = os.getenv('DATABASE_URL')  # For Heroku
#     if DATABASE_URL:
#         return psycopg2.connect(DATABASE_URL, sslmode='require')
#     else:
#         return psycopg2.connect(
#             dbname=os.getenv('DB_NAME', 'surf_forecast'),
#             user=os.getenv('DB_USER', 'your_local_user'),
#             host=os.getenv('DB_HOST', 'localhost'),
#             port=os.getenv('DB_PORT', '5432')
#         )

# def fetch_marine_data(lat, lng, location_id):
#     api_key = os.getenv('API_KEY')
    
#     if not api_key:
#         print('Error: API key not found')
#         return
        
#     base_url = "http://api.worldweatheronline.com/premium/v1/marine.ashx"
#     params = {
#         'key': api_key,
#         'format': 'json',
#         'q': f'{lat},{lng}',
#         'tide': 'yes',
#     }

#     response = requests.get(base_url, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         print("API Response:", data)

#         try:
#             if 'data' in data and data['data'].get('weather'):
#                 weather_data = data['data']['weather']
#                 insert_surf_data(location_id, weather_data)
#                 insert_tide_data(location_id, weather_data)
#             else:
#                 print("Warning: 'weather' data not found in the API response.")
#         except KeyError as e:
#             print(f"KeyError: {str(e)} - Data not found in the API response.")

# def insert_surf_data(location_id, weather_data):
#     conn = None
#     cursor = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         cursor.execute("DELETE FROM surf_data WHERE location_id = %s", (location_id,))

#         for surf_data in weather_data:
#             date = surf_data.get('date')
#             date = datetime.strptime(date, '%Y-%m-%d').date()

#             sunrise = surf_data['astronomy'][0].get('sunrise')
#             sunset = surf_data['astronomy'][0].get('sunset')

#             if sunrise:
#                 sunrise = datetime.strptime(sunrise, '%I:%M %p').time()
#             else:
#                 sunrise = None

#             if sunset:
#                 sunset = datetime.strptime(sunset, '%I:%M %p').time()
#             else:
#                 sunset = None

#             for hourly_data in surf_data.get('hourly', []):
#                 time = hourly_data.get('time')

#                 if time == '00:00' or time == '0':
#                     continue

#                 temp_f = hourly_data.get('tempF')
#                 wind_speed = hourly_data.get('windspeedMiles')
#                 wind_dir_degree = hourly_data.get('winddirDegree')
#                 wind_dir_16pt = hourly_data.get('winddir16Point')
#                 weather_desc = hourly_data['weatherDesc'][0].get('value')
#                 swell_height_ft = hourly_data.get('swellHeight_ft')
#                 swell_dir = hourly_data.get('swellDir')
#                 swell_period = hourly_data.get('swellPeriod_secs')
#                 water_temp_f = hourly_data.get('waterTemp_F')
#                 swell_dir_16pt = hourly_data.get('swellDir16Point')

#                 if not temp_f or temp_f == '0':
#                     continue
#                 if not wind_speed or wind_speed == '0':
#                     continue
#                 if not swell_height_ft or swell_height_ft == '0':
#                     continue
#                 if not wind_dir_degree:
#                     continue
#                 if not swell_dir:
#                     continue

#                 values = (
#                     location_id, date, time, temp_f, wind_speed, wind_dir_degree,
#                     wind_dir_16pt, weather_desc, swell_height_ft, swell_dir,
#                     swell_period, water_temp_f, sunrise, sunset, swell_dir_16pt
#                 )

#                 cursor.execute(
#                     '''INSERT INTO surf_data (
#                         location_id, date, time, tempf, windspeedmiles, winddirdegree,
#                         winddir16point, weatherdesc, swellheight_ft, swelldir,
#                         swellperiod_secs, watertemp_f, sunrise, sunset, swelldir16point
#                     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
#                     values
#                 )

#         conn.commit()

#     except Exception as e:
#         print(f"Error inserting surf data: {str(e)}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# def insert_tide_data(location_id, weather_data):
#     conn = None
#     cursor = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         cursor.execute("DELETE FROM tide_data WHERE location_id = %s", (location_id,))

#         for weather in weather_data:
#             if 'tides' in weather:
#                 tide_data = weather['tides'][0]['tide_data']
#                 tide_date = weather.get('date')
#                 tide_date = datetime.strptime(tide_date, '%Y-%m-%d').date()

#                 for tide_event in tide_data:
#                     tide_time = datetime.strptime(tide_event['tideTime'], '%I:%M %p').time()
#                     tide_height = tide_event.get('tideHeight_mt')
#                     tide_type = tide_event.get('tide_type')
                    
#                     print("Inserting tide values:", (location_id, tide_time, tide_height, tide_type, tide_date))

#                     cursor.execute(
#                         '''INSERT INTO tide_data (
#                             location_id, tide_time, tide_height_mt, tide_type, tide_date
#                         ) VALUES (%s, %s, %s, %s, %s)''',
#                         (location_id, tide_time, tide_height, tide_type, tide_date)
#                     )

#         conn.commit()

#     except Exception as e:
#         print(f"Error inserting tide data: {str(e)}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# def process_all_locations():
#     conn = None
#     cursor = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         cursor.execute('SELECT id, latitude, longitude FROM locations')
#         locations = cursor.fetchall()

#         for location in locations:
#             location_id, lat, lng = location
#             fetch_marine_data(lat, lng, location_id)

#     except Exception as e:
#         print(f"Error: {str(e)}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# # Run for all locations
# process_all_locations()




import requests
import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta



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
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    conn = None
    cursor = None
    try:
        if DATABASE_URL:
            conn = psycopg2.connect(DATABASE_URL, sslmode='require')  # Use Heroku DB
        else:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME', 'surf_forecast'),
                user=os.getenv('DB_USER', 'your_local_user'),
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432')
            )

        cursor = conn.cursor()

        cursor.execute("DELETE FROM surf_data WHERE location_id = %s", (location_id,))

        for surf_data in weather_data:
            # Iterate over all weather data for each date
            date = surf_data.get('date')
            date = datetime.strptime(date, '%Y-%m-%d').date()  # Convert to datetime.date object

            sunrise = surf_data['astronomy'][0].get('sunrise')
            sunset = surf_data['astronomy'][0].get('sunset')

            if sunrise:
                sunrise = datetime.strptime(sunrise, '%I:%M %p').time()  # Convert to time
            else:
                sunrise = None

            if sunset:
                sunset = datetime.strptime(sunset, '%I:%M %p').time()  # Convert to time
            else:
                sunset = None

            for hourly_data in surf_data.get('hourly', []):
                time = hourly_data.get('time')

                # Skip invalid time format
                if time == '00:00' or time == '0':
                    continue

                temp_f = hourly_data.get('tempF')
                wind_speed = hourly_data.get('windspeedMiles')
                wind_dir_degree = hourly_data.get('winddirDegree')
                wind_dir_16pt = hourly_data.get('winddir16Point')
                weather_desc = hourly_data['weatherDesc'][0].get('value')
                swell_height_ft = hourly_data.get('swellHeight_ft')
                swell_dir = hourly_data.get('swellDir')
                swell_period = hourly_data.get('swellPeriod_secs')
                water_temp_f = hourly_data.get('waterTemp_F')
                swell_dir_16pt = hourly_data.get('swellDir16Point')

                # Filter out invalid values
                if not temp_f or temp_f == '0':
                    continue
                if not wind_speed or wind_speed == '0':
                    continue
                if not swell_height_ft or swell_height_ft == '0':
                    continue
                if not wind_dir_degree:
                    continue
                if not swell_dir:
                    continue

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
