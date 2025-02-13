# import requests
# import os
# import psycopg2
# from dotenv import load_dotenv
# from datetime import datetime
# from urllib.parse import urlparse

# if os.getenv('ENV') != 'production':
#     load_dotenv('config.env')

# def fetch_tide(lat, lng, location_id):
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
#                 insert_tide_data(location_id, weather_data)
#             else:
#                 print("Warning: 'weather' data not found in the API response.")
#         except KeyError as e:
#             print(f"KeyError: {str(e)} - Tide data not found in the API response.")

# def insert_tide_data(location_id, weather_data):
#     DATABASE_URL = os.getenv('DATABASE_URL')

#     try:
#         if DATABASE_URL:
#             result = urlparse(DATABASE_URL)
#             conn = psycopg2.connect(
#                 dbname=result.path[1:],  
#                 user=result.username,
#                 password=result.password,
#                 host=result.hostname,
#                 port=result.port
#             )
#         else:
#             conn = psycopg2.connect(
#                 dbname=os.getenv('DB_NAME', 'surf_forecast'),
#                 user=os.getenv('DB_USER', 'orlandosantos'),
#                 host=os.getenv('DB_HOST', 'localhost'),
#                 port=os.getenv('DB_PORT', '5432')
#             )

#         cursor = conn.cursor()

#         # Delete existing tide data for the location
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
#         print(f"Error: {str(e)}")

#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

        
# def get_db_connection():
#     db_url = os.getenv('DATABASE_URL')
#     if db_url:
#         url_parts = urlparse(db_url)
#         return psycopg2.connect(
#             database=url_parts.path[1:],
#             user=url_parts.username,
#             password=url_parts.password,
#             host=url_parts.hostname,
#             port=url_parts.port
#         )
#     else:
#         raise Exception("DATABASE_URL not set in production")


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
#             fetch_tide(lat, lng, location_id)

#     except Exception as e:
#         print(f"Error: {str(e)}")

#     finally:
#         # Ensure cursor and conn exist before attempting to close them
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# # Run for all locations
# process_all_locations()
