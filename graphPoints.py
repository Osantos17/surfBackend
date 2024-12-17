import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime, timedelta, date
import mysql.connector # type: ignore


load_dotenv('config.env')

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')

# Use them in your database connection code
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=int(DB_PORT)
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def fetch_tide_data():
    """Fetch tide data from the database."""
    conn = get_db_connection()
    if conn is None:
        return []
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date
        FROM graph_data
        ORDER BY location_id, id
    """)
    tide_data = cursor.fetchall()
    conn.close()
    return tide_data

def fetch_latest_tide_date():
    """Fetch the most recent tide date from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(graph_date) FROM graph_points
    """)
    earliest_tide_date = cursor.fetchone()[0]
    conn.close()
    
    # If there's no data, default to today
    if not earliest_tide_date:
        earliest_tide_date = datetime.now().date()
    
    # Add one day to the fetched date
    earliest_tide_date_plus_one = earliest_tide_date - timedelta(days=1)

    print(f"Earliest tide date fetched: {earliest_tide_date}")
    print(f"Processing starts from: {earliest_tide_date_plus_one}")
    return earliest_tide_date_plus_one

def get_next_multiple_of_60(num):
    """Calculate the next multiple of 60 greater than or equal to num."""
    return ((num // 60) + 1) * 60

def generate_z_sequence(x, f):
    """Generate the z sequence based on the numeric value of x and f."""
    z = []
    current_value = get_next_multiple_of_60(x)
    while current_value <= f:
        z.append(current_value)
        current_value += 60
    return z

def interpolate_heights(x, y, tide_height_x, tide_height_y, z_sequence):
    """Interpolate heights for each value in z_sequence."""
    interpolated_values = []
    for z in z_sequence:
        interpolated_height = tide_height_x + ((z - x) / (y - x)) * (tide_height_y - tide_height_x)
        interpolated_values.append([z, round(interpolated_height, 2)])
    return interpolated_values

def adjust_numeric_values(interpolated_values, current_date):
    """Adjust numeric values based on the presence of 1440 and convert to HH:MM format."""
    adjusted_values = []
    for numeric, height in interpolated_values:
        # If numeric time is >= 1440, it indicates a time after midnight
        if numeric >= 1440:
            # Adjust the time by subtracting 1440 minutes (crosses midnight)
            numeric -= 1440
            current_date += timedelta(days=1)  # Move to the next day

        # Convert numeric time to 'HH:MM' format
        time_obj = timedelta(minutes=numeric)
        time_str = str(time_obj)[:-3]  # Remove seconds part, leaving 'HH:MM'

        # Append the adjusted time, height, and date
        adjusted_values.append([time_str, height, current_date.strftime('%Y-%m-%d')])

    return adjusted_values



def insert_into_graph_points(location_id, graph_time, tide_height, graph_date, tide_type=None):
    """Insert data into the graph_points table, ensuring no duplicate entries."""
    conn = get_db_connection()
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO graph_points (location_id, graph_time, tide_height, graph_date, tide_type)
            VALUES (%s, %s, %s, %s, %s)
        """, (location_id, graph_time, tide_height, graph_date, tide_type))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        conn.close()

def process_tide_entries(tide_data, start_date):
    """Process and insert entries into the graph_points table."""
    if isinstance(start_date, date):
        start_date = datetime.combine(start_date, datetime.min.time())
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    data_by_location = {}
    for row in tide_data:
        id, location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date = row
        tide_time_str = tide_time.strftime('%H:%M:%S') if isinstance(tide_time, datetime) else tide_time
        tide_date_str = tide_date.strftime('%Y-%m-%d') if isinstance(tide_date, datetime) else tide_date
        entry = (id, tide_time_str, tide_time_numeric, tide_height_mt, tide_type, tide_date_str)

        if location_id not in data_by_location:
            data_by_location[location_id] = []
        data_by_location[location_id].append(entry)

    # Process each location's tide data
    for location_id, entries in data_by_location.items():
        print(f"Processing Location ID: {location_id}")
        current_date = start_date  # Start from the provided start date for each location_id

        # Process tide data entries for each location
        for i in range(len(entries) - 1):
            id1, tide_time_str1, x, tide_height_mt1, tide_type1, tide_date_str1 = entries[i]
            _, _, y, tide_height_mt2, _, _ = entries[i + 1]

            f = y + 1440 if x > y else y
            z_sequence = generate_z_sequence(x, f)
            interpolated_values = interpolate_heights(x, f, tide_height_mt1, tide_height_mt2, z_sequence)
            adjusted_values = adjust_numeric_values(interpolated_values, current_date)

            insert_into_graph_points(location_id, tide_time_str1, tide_height_mt1, tide_date_str1, tide_type1)

            for time_str, height, date_str in adjusted_values:
                insert_into_graph_points(location_id, time_str, height, date_str)

        # Process the last tide entry for this location
        id_last, tide_time_str_last, x_last, tide_height_mt_last, tide_type_last, tide_date_str_last = entries[-1]
        insert_into_graph_points(location_id, tide_time_str_last, tide_height_mt_last, tide_date_str_last, tide_type_last)

        
def delete_all_graph_points():
    """Delete all records from the graph_points table."""
    conn = get_db_connection()
    if conn is None:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM graph_points")
        conn.commit()
        print("All data deleted from graph_points.")
    except psycopg2.Error as e:
        print(f"Error deleting data: {e}")
        conn.rollback()
    finally:
        conn.close()
        

def main():
    """Main function to fetch data and process entries."""
    # Delete all previous data from graph_points
    delete_all_graph_points()

    # Fetch tide data and process if available
    tide_data = fetch_tide_data()
    if not tide_data:
        print("No tide data available.")
        return

    # Fetch the latest tide date and process the entries
    start_date = fetch_latest_tide_date() or datetime.now().date()
    print(f"Processing from start date: {start_date}")
    process_tide_entries(tide_data, start_date)
    print("Data processing completed.")

    

if __name__ == "__main__":
    main()
