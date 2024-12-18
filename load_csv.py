import csv
import psycopg2
import os

def load_locations():
    # Get Heroku database URL from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return

    try:
        # Connect to the Heroku PostgreSQL database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Open the CSV file
        with open('csv/locations.csv', mode='r') as file:
            reader = csv.DictReader(file)

            # Iterate over each row in the CSV and insert into the database
            for row in reader:
                cur.execute("""
                    INSERT INTO locations (id, location_name, latitude, longitude, 
                                           preferred_wind_dir_min, preferred_wind_dir_max, 
                                           preferred_swell_dir_min, preferred_swell_dir_max, 
                                           bad_swell_dir_min, bad_swell_dir_max, wavecalc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    row['wavecalc']
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
