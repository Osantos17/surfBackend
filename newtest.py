import psycopg2
from datetime import datetime, timedelta, date

def get_db_connection():
    """Establish a connection to the database."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

def fetch_tide_data():
    """Fetch tide data from the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date
        FROM graph_data
        ORDER BY location_id, id
    """)
    tide_data = cursor.fetchall()
    conn.close()
    return tide_data

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

def adjust_numeric_values(interpolated_values):
    """Adjust numeric values based on the presence of 1440 and convert to HH:MM format."""
    adjusted_values = []
    subtract_1440 = False
    for numeric, height in interpolated_values:
        if numeric == 1440:
            adjusted_values.append(["00:00", height])
            subtract_1440 = True
        else:
            if subtract_1440:
                numeric -= 1440
            time_str = str(timedelta(minutes=numeric))[:-3]
            adjusted_values.append([time_str, height])
    return adjusted_values

def process_tide_entries(tide_data, start_date):
    """Process and print entries for each location group with calculated and adjusted tide info."""
    if isinstance(start_date, date):
        start_date = start_date.strftime("%Y-%m-%d")
    
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    
    data_by_location = {}
    for row in tide_data:
        id, location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date = row
        tide_time_str = tide_time.strftime('%H:%M:%S') if isinstance(tide_time, datetime) else tide_time
        tide_date_str = tide_date.strftime('%Y-%m-%d') if isinstance(tide_date, datetime) else tide_date
        entry = (id, tide_time_str, tide_time_numeric, tide_height_mt, tide_type, tide_date_str)

        if location_id not in data_by_location:
            data_by_location[location_id] = []
        data_by_location[location_id].append(entry)
    
    for location_id, entries in data_by_location.items():
        print(f"Location ID: {location_id}")
        
        current_date = start_date_dt
        
        for i in range(len(entries) - 1):
            id1, tide_time_str1, x, tide_height_mt1, tide_type1, tide_date_str1 = entries[i]
            _, _, y, tide_height_mt2, _, _ = entries[i + 1]

            f = y + 1440 if x > y else y

            z_sequence = generate_z_sequence(x, f)
            
            interpolated_values = interpolate_heights(x, f, tide_height_mt1, tide_height_mt2, z_sequence)
            
            adjusted_values = adjust_numeric_values(interpolated_values)

            print(f" Tide Source:  [{tide_time_str1}, {tide_height_mt1}, {tide_date_str1}, {tide_type1}]")
            
            for idx, value in enumerate(adjusted_values, start=1):
                time_str, height = value
                if time_str == '00:00':
                    current_date += timedelta(days=1)
                    
                print(f"  Tide Info {idx}: ['{time_str}', {height}, {current_date.strftime('%Y-%m-%d')}]")
        
        id_last, tide_time_str_last, x_last, tide_height_mt_last, tide_type_last, tide_date_str_last = entries[-1]
        print(f" Tide Source: [{tide_time_str_last}, {tide_height_mt_last}, {tide_date_str_last}, {tide_type_last}]")

def main():
    """Main function to fetch data and process entries."""
    tide_data = fetch_tide_data()
    # Dynamically set start_date as the date of the first entry in tide_data
    start_date = tide_data[0][-1] if tide_data else "2024-11-11"  # Default if tide_data is empty
    process_tide_entries(tide_data, start_date)

# Run the main function
if __name__ == "__main__":
    main()
