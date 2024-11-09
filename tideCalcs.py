import psycopg2
import datetime

def get_db_connection():
    """Establish a connection to the database."""
    return psycopg2.connect(
        dbname="surf_forecast",  
        user="orlandosantos",    
        host="localhost",        
        port="5432"
    )

def fetch_tide_data(date):
    """Fetch tide data for a given date and the last tide data from the previous day."""
    try:
        # Establish database connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to get tide data for the target date
        cursor.execute("""
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM graph_data
            WHERE tide_date = %s
            ORDER BY tide_time
        """, (date,))
        tide_data = cursor.fetchall()

        # Query to get the last tide entry of the previous day
        previous_date = date - datetime.timedelta(days=1)
        cursor.execute("""
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM graph_data
            WHERE tide_date = %s
            ORDER BY tide_time DESC
            LIMIT 1
        """, (previous_date,))
        previous_day_data = cursor.fetchone()

        return previous_day_data, tide_data

    except Exception as e:
        print(f"Error fetching tide data: {e}")
    finally:
        cursor.close()
        conn.close()

def interpolate_tide_height(tide1, tide2, target_time):
    """Perform linear interpolation between two tide data points for a target time."""
    # Convert times to datetime objects for interpolation
    time1 = datetime.datetime.combine(tide1[4], tide1[1])  # First tide point
    time2 = datetime.datetime.combine(tide2[4], tide2[1])  # Second tide point
    target_time = datetime.datetime.combine(tide1[4], target_time)  # Target time

    # Calculate the time difference in seconds
    time_diff_total = (time2 - time1).total_seconds()
    time_diff_target = (target_time - time1).total_seconds()
    
    # Calculate the interpolated height
    height_diff = tide2[2] - tide1[2]
    interpolated_height = tide1[2] + (height_diff * time_diff_target / time_diff_total)
    return interpolated_height

def calculate_hourly_tides(start_time, date):
    """Calculate tide heights at each whole hour from a start time up to midnight."""
    previous_day_data, tide_data = fetch_tide_data(date)

    if not previous_day_data or not tide_data:
        print("Insufficient data for interpolation.")
        return

    # Add the last tide of the previous day to the current day's tide list
    tide_data.insert(0, previous_day_data)

    # Generate tide heights at each hour up to midnight
    current_time = start_time
    end_time = datetime.datetime.combine(date, datetime.time(0, 0))

    while current_time <= end_time:
        # Find the closest surrounding data points for the current time
        closest_before = None
        closest_after = None

        for i in range(len(tide_data) - 1):
            time1 = datetime.datetime.combine(tide_data[i][4], tide_data[i][1])
            time2 = datetime.datetime.combine(tide_data[i + 1][4], tide_data[i + 1][1])
            if time1 <= current_time <= time2:
                closest_before = tide_data[i]
                closest_after = tide_data[i + 1]
                break

        # If valid surrounding tide data is found, interpolate the tide height
        if closest_before and closest_after:
            interpolated_height = interpolate_tide_height(closest_before, closest_after, current_time.time())
            print(f"Tide height at {current_time.strftime('%H:%M')} on {date.strftime('%Y-%m-%d')}: {interpolated_height:.2f} meters")
        else:
            print(f"No surrounding data to interpolate for {current_time.strftime('%H:%M')}.")

        # Increment time by one hour
        current_time += datetime.timedelta(hours=1)

# Define the target date and start time
target_date = datetime.date(2024, 11, 8)
start_time = datetime.datetime.combine(target_date - datetime.timedelta(days=1), datetime.time(21, 0))

# Calculate tide heights at each hour from start_time through midnight
calculate_hourly_tides(start_time, target_date)
