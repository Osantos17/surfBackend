import csv
import psycopg2
import os

def safe_int(value):
    
    if value == '' or value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None 

def update_locations():
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        with open('csv/locations.csv', mode='r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                print(row)

                cur.execute("""
                    UPDATE locations
                    SET 
                        location_name = %s,
                        latitude = %s,
                        longitude = %s,
                        preferred_wind_dir_min = %s,
                        preferred_wind_dir_max = %s,
                        preferred_swell_dir_min = %s,
                        preferred_swell_dir_max = %s,
                        bad_swell_dir_min = %s,
                        bad_swell_dir_max = %s,
                        wavecalc = %s,
                        region = %s,
                        reef = %s  -- Add the new 'reef' column here
                    WHERE id = %s
                """, (
                    row['location_name'],
                    row['latitude'],
                    row['longitude'],
                    safe_int(row['preferred_wind_dir_min']), 
                    safe_int(row['preferred_wind_dir_max']), 
                    safe_int(row['preferred_swell_dir_min']),
                    safe_int(row['preferred_swell_dir_max']),
                    safe_int(row['bad_swell_dir_min']),  
                    safe_int(row['bad_swell_dir_max']),  
                    row['wavecalc'],
                    row['region'],
                    row['reef'].lower() == 'true', 
                    row['id']  
                ))

        conn.commit()
        cur.close()
        conn.close()
        print("Locations updated successfully.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    update_locations()