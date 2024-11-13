import psycopg2
from datetime import datetime, timedelta
from test import fetch_tide_data


def get_db_connection():
    """Establish a connection to the database."""
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

def clear_graph_points():
    """Clear existing records from graph_points table before inserting new data."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM graph_points")
    conn.commit()
    conn.close()

def insert_into_graph_points(location_id, graph_time, tide_height, graph_date, tide_type=None):
    """Insert data into the graph_points table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO graph_points (location_id, graph_time, tide_height, graph_date, tide_type)
        VALUES (%s, %s, %s, %s, %s)
    """, (location_id, graph_time, tide_height, graph_date, tide_type))
    conn.commit()
    conn.close()

def process_tide_entries(tide_data, start_date):
    """Process tide data and insert it into graph_points."""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
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
        for i in range(len(entries) - 1):
            id1, tide_time_str1, x, tide_height_mt1, tide_type1, tide_date_str1 = entries[i]
            _, _, y, tide_height_mt2, _, _ = entries[i + 1]
            f = y + 1440 if x > y else y

            z_sequence = generate_z_sequence(x, f)
            interpolated_values = interpolate_heights(x, f, tide_height_mt1, tide_height_mt2, z_sequence)
            adjusted_values = adjust_numeric_values(interpolated_values)

            insert_into_graph_points(location_id, tide_time_str1, tide_height_mt1, tide_date_str1, tide_type1)

            for value in adjusted_values:
                time_str, height = value
                if time_str == '00:00':
                    current_date += timedelta(days=1)
                insert_into_graph_points(location_id, time_str, height, current_date.strftime('%Y-%m-%d'))

        # Insert the last entry in the series
        id_last, tide_time_str_last, _, tide_height_mt_last, tide_type_last, tide_date_str_last = entries[-1]
        insert_into_graph_points(location_id, tide_time_str_last, tide_height_mt_last, tide_date_str_last, tide_type_last)

def main():
    clear_graph_points()  # Clear table before inserting new data
    tide_data = fetch_tide_data()
    start_date = "2024-11-08"
    process_tide_entries(tide_data, start_date)

if __name__ == "__main__":
    main()
