import csv
import psycopg2
import os

def safe_int(value):
    # Return None if the value is empty or not a valid integer
    if value == '' or value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None  # Return None if the value cannot be converted to an integer

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

            # Iterate over each row in the CSV and insert into the database
            for row in reader:
                # Debugging: Print the row before insertion to check data
                print(row)

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
                    safe_int(row['bad_swell_dir_min']),  # Convert empty string to None (NULL)
                    safe_int(row['bad_swell_dir_max']),  # Convert empty string to None (NULL)
                    row['wavecalc'],
                    row['region'], 
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
