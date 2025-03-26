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

                # First try to update
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
                        reef = %s
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

                # If no rows were updated, insert a new record
                if cur.rowcount == 0:
                    cur.execute("""
                        INSERT INTO locations (
                            id,
                            location_name,
                            latitude,
                            longitude,
                            preferred_wind_dir_min,
                            preferred_wind_dir_max,
                            preferred_swell_dir_min,
                            preferred_swell_dir_max,
                            bad_swell_dir_min,
                            bad_swell_dir_max,
                            wavecalc,
                            region,
                            reef
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        row['id'],
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
                        row['reef'].lower() == 'true'
                    ))

        conn.commit()
        cur.close()
        conn.close()
        print("Locations updated/inserted successfully.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    update_locations()