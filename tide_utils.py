import psycopg2
from datetime import datetime, timedelta
import numpy as np

def convert_to_24hr_format(time_str: str) -> str:
    """Converts 12-hour time format (e.g., '02:30 PM') to 24-hour format (e.g., '14:30')."""
    try:
        time_obj = datetime.strptime(time_str, '%I:%M %p')  # Assumes time_str is in 12-hour format with AM/PM
        return time_obj.strftime('%H:%M')  # Convert to 24-hour format
    except ValueError:
        print(f"Invalid time format: {time_str}")
        return time_str

def interpolate_tide_heights(start_time, end_time, start_height, end_height):
    """Interpolate tide heights between two points."""
    delta_hours = int((end_time - start_time).total_seconds() / 3600)
    times = [start_time + timedelta(hours=i) for i in range(delta_hours + 1)]
    heights = np.linspace(start_height, end_height, delta_hours + 1)  # Linear interpolation
    return list(zip(times, heights))

def get_last_tide_entry(location_id):
    """Retrieve the last tide entry for a given location ID."""
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()
    
    # Parameterized query with proper casting and formatting
    query = """
        SELECT 
            location_id, 
            TO_CHAR(tide_time, 'HH24:MI') AS tide_time_formatted, 
            tide_height_mt
        FROM tide_data
        WHERE location_id = %s
        ORDER BY tide_date DESC
        LIMIT 1;
    """

    
    
    cursor.execute(query, (location_id,))  # Pass location_id as a parameter
    
    last_tide_entry = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return last_tide_entry

def calculate_hourly_tide_data(lat, lng, tide_data):
    """Calculate hourly tide heights for a location and date range."""
    previous_data, current_data, next_data = tide_data
    
    hourly_tide_points = []
    
    # Interpolate between the previous day's data and current day's data
    for i in range(1, len(previous_data)):
        start_time = previous_data[i-1][0]
        end_time = previous_data[i][0]
        start_height = previous_data[i-1][1]
        end_height = previous_data[i][1]
        
        interpolated_points = interpolate_tide_heights(start_time, end_time, start_height, end_height)
        hourly_tide_points.extend(interpolated_points)
    
    # Interpolate between the current day's data and next day's data
    for i in range(1, len(next_data)):
        start_time = current_data[i-1][0]
        end_time = next_data[i][0]
        start_height = current_data[i-1][1]
        end_height = next_data[i][1]
        
        interpolated_points = interpolate_tide_heights(start_time, end_time, start_height, end_height)
        hourly_tide_points.extend(interpolated_points)
    
    return hourly_tide_points
