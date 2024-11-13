import psycopg2
from datetime import datetime, timedelta

def get_db_connection():
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host="localhost",
        port="5432"
    )

def fetch_tide_data():
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT id, tide_time, tide_height_mt FROM graph_data ORDER BY id ASC")
    data = cursor.fetchall()
    connection.close()

    arbitrary_date = datetime(2000, 1, 1)

    for i in range(len(data) - 1):  # Iterate normally for all except the last entry
        x_id, x_time, x_height = data[i]
        y_id, y_time, y_height = data[i + 1]

        x_minute_only = x_time.strftime("%M")
        current_entry = [f"00:{x_minute_only}", x_height]

        # Print the current entry during each iteration
        print(f"\nSet {i + 1}:")
        print("x_minute_only:", x_minute_only)
        print("current_entry:", current_entry)

    # Handle the last entry separately
    if data:
        x_id, x_time, x_height = data[-1]
        x_minute_only = x_time.strftime("%M")
        current_entry = [f"00:{x_minute_only}", x_height]

        # Print the final set as Set 60
        print(f"\nSet {len(data)}:")
        print("x_minute_only:", x_minute_only)
        print("current_entry:", current_entry)

# Run the function to fetch and display data
fetch_tide_data()
