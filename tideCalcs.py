import psycopg2
from datetime import datetime, timedelta

# Function to get the database connection
def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

# Function to round up the time to the nearest hour
def round_up_to_hour(time_obj):
    """Round time to the next full hour if minutes > 0."""
    # Handle potential day transition at midnight
    base_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    time_as_datetime = base_date + timedelta(hours=time_obj.hour, minutes=time_obj.minute)
    
    # If there's a remainder in minutes, round up
    if time_obj.minute > 0:
        time_as_datetime += timedelta(hours=1)

    # Check if it rounds to midnight (24:00), change it to 00:00 instead
    if time_as_datetime.hour == 24:
        time_as_datetime = time_as_datetime.replace(hour=0, minute=0)

    # Ensure it's rounded to the next hour, reset minutes and seconds to 0
    return time_as_datetime.replace(minute=0, second=0, microsecond=0)


# Linear interpolation function
def linear_interpolation(t1, t2, y1, y2, t):
    """Interpolate tide height at time t given two times (t1, t2) and tide heights (y1, y2)."""
    t1_minutes = t1.hour * 60 + t1.minute
    t2_minutes = t2.hour * 60 + t2.minute
    t_minutes = t.hour * 60 + t.minute
    
    if t1_minutes != t2_minutes:
        return y1 + (t_minutes - t1_minutes) * (y2 - y1) / (t2_minutes - t1_minutes)
    return y1  # If times are identical, return the tide height

# Function to calculate tide heights for each location
def calculate_tide_heights():
    try:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch all tide data ordered by id
        cursor.execute('SELECT id, location_id, tide_time, tide_height_mt, tide_type, tide_date FROM graph_data ORDER BY id')
        tide_data = cursor.fetchall()

        # Open the file to write the results
        with open('tide_heights_output.txt', 'w') as f:
            for i in range(len(tide_data) - 1):
                id_1, loc_1, time_1, height_1, _, date_1 = tide_data[i]
                id_2, loc_2, time_2, height_2, _, date_2 = tide_data[i + 1]
                
                # Round the tide time of the first entry
                rounded_time_1 = round_up_to_hour(time_1)
                time_2 = datetime.combine(datetime.today(), time_2)  # Combine time_2 with today's date

                # Perform linear interpolation for each hour between rounded time and next time
                current_time = rounded_time_1
                while current_time < time_2:  # Loop from rounded time to the second time
                    tide_height = linear_interpolation(rounded_time_1, time_2, height_1, height_2, current_time)
                    f.write(f"{loc_1}, {current_time.strftime('%H:%M')}, {tide_height:.2f}\n")
                    print(f"{loc_1}, {current_time.strftime('%H:%M')}, {tide_height:.2f}")
                    current_time += timedelta(hours=1)  # Increment by one hour

                # Write the second tide data point (id_2) after interpolation
                f.write(f"{loc_2}, {time_2.strftime('%H:%M')}, {height_2:.2f}\n")
                print(f"{loc_2}, {time_2.strftime('%H:%M')}, {height_2:.2f}")

        print("Tide heights calculation complete. Output written to 'tide_heights_output.txt'.")

    except Exception as e:
        print(f"Error calculating tide heights: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Run the calculation
calculate_tide_heights()
