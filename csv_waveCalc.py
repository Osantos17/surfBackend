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

def update_locations():
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Load data from the CSV file
        with open('csv/locations.csv', mode='r') as file:
            reader = csv.DictReader(file)

            # Iterate over each row in the CSV and update the database
            for row in reader:
                # Debugging: Print the row before updating to check data
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
                        region = %s
                    WHERE id = %s
                """, (
                    row['location_name'],
                    row['latitude'],
                    row['longitude'],
                    safe_int(row['preferred_wind_dir_min']),  # Use safe_int to handle empty or invalid values
                    safe_int(row['preferred_wind_dir_max']),  # Use safe_int to handle empty or invalid values
                    safe_int(row['preferred_swell_dir_min']),  # Use safe_int for swell direction
                    safe_int(row['preferred_swell_dir_max']),  # Use safe_int for swell direction
                    safe_int(row['bad_swell_dir_min']),  # Convert empty string to None (NULL)
                    safe_int(row['bad_swell_dir_max']),  # Convert empty string to None (NULL)
                    row['wavecalc'],
                    row['region'],
                    row['id']  # Use the id to identify the row to update
                ))

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
        print("Locations updated successfully.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    update_locations()