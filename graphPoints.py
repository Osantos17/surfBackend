import psycopg2
import sqlite3
from datetime import datetime, timedelta

import sqlite3

def create_graph_points_table():
    conn = sqlite3.connect('your_database.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS graph_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER,
            graph_date DATE NOT NULL,
            graph_time TIME NOT NULL,
            tide_height NUMERIC(5, 2) NOT NULL,
            FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE
        )
    ''')
    conn.commit()
    conn.close()
    print("Created graph_points table if not exists.")

# Run this function once to ensure the table is created
create_graph_points_table()


def get_tide_data(location_id, date):
    """Fetches tide data for a given location and date, including the previous day's last tide entry if necessary."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="orlandosantos",
            host='localhost',
            port='5432'
        )
        cursor = conn.cursor()
        
        # Fetch tide data for the specified date from tide_data
        cursor.execute('''
            SELECT tide_time, tide_height_mt
            FROM tide_data
            WHERE location_id = %s AND tide_date = %s
            ORDER BY tide_time
        ''', (location_id, date))
        tide_data = cursor.fetchall()

        # Check if this is the current day and needs the last tide data from the previous day
        if date == datetime.now().date():
            cursor.execute('''
                SELECT tide_time, tide_height_mt
                FROM boundary_tide_data
                WHERE location_id = %s AND tide_date = %s
                ORDER BY tide_time DESC
                LIMIT 1
            ''', (location_id, date - timedelta(days=1)))
            boundary_data = cursor.fetchone()
            
            # Add the last boundary data as the first point if available
            if boundary_data:
                tide_data.insert(0, boundary_data)
        
        cursor.close()
        return tide_data

    except Exception as e:
        print(f"Error fetching tide data: {e}")
        return []

    finally:
        if conn:
            conn.close()
            
# Example usage to fetch data for today's date
location_id = 1
date = datetime.now().date()
tide_data = get_tide_data(location_id, date)
print("Fetched tide data:", tide_data)

def interpolate_hourly_tides(tide_data, date):
    """Interpolates tide heights for each hour from the given tide data."""
    hourly_tides = {}
    
    for i in range(len(tide_data) - 1):
        # Current and next tide data points
        current_time, current_height = tide_data[i]
        next_time, next_height = tide_data[i + 1]

        # Ensure times are in datetime.time format
        if isinstance(current_time, str):
            current_time = datetime.strptime(current_time, "%H:%M").time()
        if isinstance(next_time, str):
            next_time = datetime.strptime(next_time, "%H:%M").time()

        # Convert times to datetime objects for easier manipulation
        current_time_dt = datetime.combine(date, current_time)
        next_time_dt = datetime.combine(date, next_time)

        # Calculate the time difference in hours
        hours_diff = int((next_time_dt - current_time_dt).total_seconds() // 3600)

        # Linear interpolation for each hour between current and next tide points
        for h in range(hours_diff + 1):  # +1 to include next point as boundary
            hour_time = current_time_dt + timedelta(hours=h)
            interpolation_ratio = h / hours_diff if hours_diff != 0 else 1
            interpolated_height = round(
                current_height + interpolation_ratio * (next_height - current_height), 2
            )
            hourly_tides[hour_time.strftime("%H:%M")] = interpolated_height

    return hourly_tides

# Example usage with the fetched tide data
interpolated_tides = interpolate_hourly_tides(tide_data, date)
print("Interpolated hourly tides:", interpolated_tides)

def insert_graph_point(conn, location_id, graph_date, graph_time, tide_height):
    """Insert a data point into the graph_points table."""
    with conn:
        conn.execute(
            """
            INSERT INTO graph_points (location_id, graph_date, graph_time, tide_height)
            VALUES (?, ?, ?, ?)
            """,
            (location_id, graph_date, graph_time, tide_height)
        )
    print(f"Inserted: {graph_date} {graph_time} - Tide Height: {tide_height}")

# Database connection
conn = sqlite3.connect('your_database.db')

# Example location_id for inserting data points
location_id = 1

# Using the interpolated tide data
graph_date = date.today()  # Use today's date, replace with desired date if needed
for time_str, tide_height in interpolated_tides.items():
    insert_graph_point(conn, location_id, graph_date, time_str, tide_height)

# Close the connection when done
conn.close()