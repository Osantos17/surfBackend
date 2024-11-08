from datetime import datetime, timedelta
import sqlite3
import psycopg2
from sqlite3 import dbapi2 as sqlite

def adapt_date(date):
    return date.strftime('%Y-%m-%d')

sqlite.register_adapter(datetime, adapt_date)

# Interpolation function to round to whole hours and cover all hours
def interpolate_hourly_tides(tide_data, date, boundary_tide_data):
    """Interpolates tide heights for each hour from the given tide data."""
    hourly_tides = {}

    # Start by adding the 00:00 tide height from boundary_tide_data if available
    if boundary_tide_data:
        boundary_time, boundary_height = boundary_tide_data
        hourly_tides['00:00'] = boundary_height  # Use boundary tide height for 00:00

    # Continue interpolation as before, starting from the first tide data entry
    for i in range(len(tide_data) - 1):
        current_time, current_height = tide_data[i]
        next_time, next_height = tide_data[i + 1]

        # Convert times to datetime objects for manipulation
        current_time_dt = datetime.combine(date, current_time)
        next_time_dt = datetime.combine(date, next_time)

        # Calculate the time difference in hours
        hours_diff = int((next_time_dt - current_time_dt).total_seconds() // 3600)

        # Interpolation for each whole hour between current and next tide points
        for h in range(hours_diff + 1):
            hour_time = (current_time_dt + timedelta(hours=h)).replace(minute=0)
            interpolation_ratio = h / hours_diff if hours_diff != 0 else 1
            interpolated_height = round(
                current_height + interpolation_ratio * (next_height - current_height), 2
            )

            # Ensure we add only unique hourly entries
            hour_str = hour_time.strftime("%H:%M")
            if hour_str not in hourly_tides:  # Prevent duplicates
                hourly_tides[hour_str] = interpolated_height

    # If there are any missing hours between the last tide data and the next boundary, add them
    for hour in range(24):
        hour_str = f"{hour:02d}:00"
        if hour_str not in hourly_tides:
            # Estimate the value based on the last available tide data (extrapolation)
            last_hour_time = datetime.combine(date, tide_data[-1][0])
            last_hour_height = tide_data[-1][1]
            hourly_tides[hour_str] = last_hour_height

    return hourly_tides


# Fetch tide data
def get_tide_data(location_id, date):
    """Fetches tide data for a given location and date."""
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
        
        # Fetch boundary tide data for the previous day's last entry to provide 00:00 value
        cursor.execute('''
            SELECT tide_time, tide_height_mt
            FROM tide_data
            WHERE location_id = %s AND tide_date = %s
            ORDER BY tide_time DESC LIMIT 1
        ''', (location_id, date - timedelta(days=1)))
        boundary_tide_data = cursor.fetchone()

        print(f"Fetched tide data: {tide_data}")
        print(f"Boundary Tide Data: {boundary_tide_data}")

        cursor.close()
        return tide_data, boundary_tide_data

    except Exception as e:
        print(f"Error fetching tide data: {e}")
        return [], []

    finally:
        if conn:
            conn.close()


# Insert function
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

# Database setup and example usage
def main():
    # Connect to SQLite
    conn = sqlite3.connect('your_database.db')

    # Example location_id for inserting data points
    location_id = 1
    graph_date = datetime.now().date()  # Today's date for example

    # Fetch data and interpolate
    tide_data, boundary_tide_data = get_tide_data(location_id, graph_date)
    interpolated_tides = interpolate_hourly_tides(tide_data, graph_date, boundary_tide_data)
    print("Interpolated hourly tides:", interpolated_tides)

    # Insert interpolated tides
    for time_str, tide_height in interpolated_tides.items():
        insert_graph_point(conn, location_id, graph_date, time_str, tide_height)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
