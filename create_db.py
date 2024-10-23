import psycopg2
import requests
import os
from dotenv import load_dotenv

load_dotenv('config.env')

def create_db():
    try:
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="orlandosantos",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        #locations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id SERIAL PRIMARY KEY,
                location_name VARCHAR(100) NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL
            )
        ''')

        #surf_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS surf_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                date DATE NOT NULL,
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
        
        # Tide_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_time VARCHAR(10),
                tide_height FLOAT,
                tide_type VARCHAR(10),
                tide_datetime TIMESTAMP
            )
        ''')

        conn.commit()
        cursor.close()
        conn.close()

        print("Database and tables created successfully!")

    except Exception as e:
        print(f"Error creating database: {e}")

def fetch_surf_data(lat, lng):
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

def insert_surf_data(location_id, data):
    try:
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="orlandosantos",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

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


        # Loop through all dates in the weather data
        for weather_data in data['data']['weather']:
            astronomy_data = weather_data['astronomy'][0]
            selected_hours = ['300', '600', '900', '1200', '1500', '1800', '2100'] 

            for hourly_data in weather_data['hourly']:
                if hourly_data['time'] in selected_hours:
                    record = (
                        location_id,
                        weather_data['date'],
                        astronomy_data['sunrise'],
                        astronomy_data['sunset'],
                        hourly_data['time'],
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
                        tide_event['tideDateTime']
                    )
                    cursor.execute(insert_tide_query, tide_record)

        conn.commit()
        cursor.close()
        conn.close()

        print("Surf data inserted successfully!")

    except Exception as e:
        print(f"Error inserting surf data: {e}")

# Example usage
if __name__ == "__main__":
    create_db()  # Create the database and tables

    # Connect to the database to retrieve existing locations
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Fetch all locations from the locations table
    cursor.execute('SELECT id, latitude, longitude FROM locations')
    locations = cursor.fetchall()

    for location in locations:
        location_id, lat, lng = location
        # Fetch the surf data for this location
        surf_data = fetch_surf_data(lat, lng)
        if surf_data:
            insert_surf_data(location_id, surf_data)

    conn.commit()
    cursor.close()
    conn.close()
