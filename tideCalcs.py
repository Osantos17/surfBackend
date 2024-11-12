import psycopg2
from datetime import datetime, timedelta

def get_db_connection():
    """Establish a connection to the database."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

def interpolate(x_height, w_height, x_time, w_time, z_times):
    """Perform linear interpolation for tide heights at given times."""
    tide_heights = []

    # Helper function to convert time strings to minutes past midnight
    def time_to_minutes(t):
        hours, minutes = map(int, t.split(":"))
        return hours * 60 + minutes

    x_minutes = time_to_minutes(x_time)
    w_minutes = time_to_minutes(w_time)
    total_time_diff = w_minutes - x_minutes

    # Interpolate for each time in z_times
    for z_time in z_times:
        z_minutes = time_to_minutes(z_time) - x_minutes  # Time in minutes relative to x_time
        height = x_height + (w_height - x_height) * z_minutes / total_time_diff
        tide_heights.append(round(height, 2))
    
    return tide_heights

def fetch_tide_data():
    """Fetch tide data and create arrays with adjusted times to start from 00:MM."""
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("""
        SELECT id, tide_time, tide_height_mt, location_id, tide_date, tide_type
        FROM graph_data 
        ORDER BY id ASC
    """)
    data = cursor.fetchall()
    connection.close()

    M = []  # Store the data table here
    L = []  # Store tide_info here

    minute_based_array = []
    time_interpolations = []
    actual_hourly_interpolations = []

    # Arbitrary date for time manipulation
    arbitrary_date = datetime(2000, 1, 1)

    # Set to track already printed IDs
    printed_ids = set()

    for i in range(len(data) - 1):
        x_id, x_time, x_height, x_location_id, x_tide_date, x_tide_type = data[i]
        y_id, y_time, y_height, y_location_id, y_tide_date, y_tide_type = data[i + 1]

        if x_id not in printed_ids:
            M.append({
                "ID": x_id,
                "Location ID": x_location_id,
                "Tide Time": x_time.strftime('%H:%M'),
                "Tide Height": x_height,
                "Tide Date": x_tide_date.strftime('%Y-%m-%d'),
                "Tide Type": x_tide_type
            })
            printed_ids.add(x_id)

        if y_id != x_id and y_id not in printed_ids:
            M.append({
                "ID": y_id,
                "Location ID": y_location_id,
                "Tide Time": y_time.strftime('%H:%M'),
                "Tide Height": y_height,
                "Tide Date": y_tide_date.strftime('%Y-%m-%d'),
                "Tide Type": y_tide_type
            })
            printed_ids.add(y_id)

        x_minute_only = x_time.strftime("%M")
        minute_based_array.append([x_id, f"00:{x_minute_only}", x_height])

        x_datetime = datetime.combine(arbitrary_date, x_time)
        y_datetime = datetime.combine(arbitrary_date, y_time)

        if x_datetime.minute > 0:
            x_rounded = (x_datetime + timedelta(hours=1)).replace(minute=0)
        else:
            x_rounded = x_datetime.replace(minute=0) + timedelta(hours=1)

        y_rounded = y_datetime.replace(minute=0)
        if y_rounded < x_rounded:
            y_rounded += timedelta(days=1)

        hour_count = int((y_rounded - x_rounded).total_seconds() / 3600) + 1

        start_time = arbitrary_date.replace(hour=1, minute=0)
        time_series = [(start_time + timedelta(hours=j)).strftime("%H:%M") for j in range(hour_count)]
        time_interpolations.append(time_series)

        actual_time_series = [(x_rounded + timedelta(hours=j)).strftime("%H:%M") for j in range(hour_count)]
        actual_hourly_interpolations.append(actual_time_series)

    for i in range(len(time_interpolations)):
        z_times = time_interpolations[i]
        actual_z_times = actual_hourly_interpolations[i]

        if i < len(minute_based_array) - 1:
            x = minute_based_array[i]
            y = minute_based_array[i + 1]

            last_z_time = datetime.strptime(z_times[-1], "%H:%M")
            y_minutes = int(y[1].split(":")[1])
            w_time = (last_z_time + timedelta(minutes=y_minutes)).strftime("%H:%M")
            w = [w_time, y[2]]

            interpolated_z_heights = interpolate(x[2], w[1], x[1], w[0], z_times)

            tide_info = [[actual_z_time, interpolated_z_height] for actual_z_time, interpolated_z_height in zip(actual_z_times, interpolated_z_heights)]
            L.append(tide_info)  # Store tide_info in L

    return M, L

# Call the function and assign the result to variables M and L
M, L = fetch_tide_data()

for i in range(max(len(M), len(L))):  # Loop through the maximum length of M or L
    if i < len(M):
        print("M object:", M[i])
    if i < len(L):
        print("L object:", L[i])
