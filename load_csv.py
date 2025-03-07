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

def clear_tables(cur):
    """
    Clear data from all related tables.
    """
    tables = [
        "boundary_tide_data",
        "surf_data",
        "tide_data",
        "graph_data",
        "graph_points",
        "locations" 
    ]
    for table in tables:
        cur.execute(f"DELETE FROM {table}")
        print(f"Cleared data from '{table}' table.")

def load_locations():
    DATABASE_URL = os.getenv('DATABASE_URL')

    if not DATABASE_URL:
        print("Error: DATABASE_URL is not set.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Clear all related tables
        clear_tables(cur)

        # Load new data into the locations table
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
                    safe_int(row['preferred_wind_dir_min']),  # Use safe_int to handle empty or invalid values
                    safe_int(row['preferred_wind_dir_max']),  # Use safe_int to handle empty or invalid values
                    safe_int(row['preferred_swell_dir_min']),  # Use safe_int for swell direction
                    safe_int(row['preferred_swell_dir_max']),  # Use safe_int for swell direction
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