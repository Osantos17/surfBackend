import csv
import psycopg2
import os

def load_locations():
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Clear the existing table data
        cur.execute("DELETE FROM locations")
        print("Existing data cleared from 'locations' table.")

        with open('csv/locations.csv', mode='r') as file:
            reader = csv.DictReader(file)
        
            for row in reader:
                region = row['region'].strip() if row['region'].strip() != '' else None  # Ensure blank is treated as NULL
                
                cur.execute("""
                    INSERT INTO locations (id, location_name, latitude, longitude, 
                                           preferred_wind_dir_min, preferred_wind_dir_max, 
                                           preferred_swell_dir_min, preferred_swell_dir_max, 
                                           bad_swell_dir_min, bad_swell_dir_max, wavecalc, region)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['id'],
                    row['location_name'],
                    row['latitude'],
                    row['longitude'],
                    row['preferred_wind_dir_min'],
                    row['preferred_wind_dir_max'],
                    row['preferred_swell_dir_min'],
                    row['preferred_swell_dir_max'],
                    row['bad_swell_dir_min'],
                    row['bad_swell_dir_max'],
                    row['wavecalc'],
                    region,  # Now using `None` for empty region values
                ))

        
        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
        print("Locations loaded successfully.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    load_locations()
