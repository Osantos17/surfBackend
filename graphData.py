import psycopg2
import datetime

def get_db_connection():
    # Get the DATABASE_URL environment variable
    DATABASE_URL = os.environ.get('DATABASE_URL')

    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        # For local testing or fallback, use local settings (adjust as needed)
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="orlandosantos",
            host="localhost",
            port="5432"
        )
    
    return conn

def time_to_numeric(tide_time):
    """Convert time to numeric value representing minutes since midnight."""
    if isinstance(tide_time, str):
        # Parse the time string (assumed format is '%H:%M')
        try:
            time_obj = datetime.datetime.strptime(tide_time, '%H:%M').time()
            return time_obj.hour * 60 + time_obj.minute
        except ValueError:
            print(f"Invalid time format: {tide_time}")
            return 0  # Return 0 if the time string format is incorrect
    elif isinstance(tide_time, datetime.time):
        return tide_time.hour * 60 + tide_time.minute
    return 0  # In case tide_time is None or invalid


def update_graph_data():
    """Fetch combined data from tide_data and boundary_tide_data tables, 
       then replace current data in graph_data with the updated data."""
    try:
        # Establish database connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch data from tide_data table
        cursor.execute("""
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM tide_data
            ORDER BY location_id, tide_date, tide_time
        """)
        tide_data_rows = cursor.fetchall()

        # Fetch data from boundary_tide_data table
        cursor.execute("""
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM boundary_tide_data
            ORDER BY location_id, tide_date, tide_time
        """)
        boundary_data_rows = cursor.fetchall()

        # Combine and sort the data
        combined_data = tide_data_rows + boundary_data_rows
        combined_data.sort(key=lambda x: (x[0], x[4], x[1]))  # Sort by location_id, date, time

        # Clear current data in graph_data table
        cursor.execute("DELETE FROM graph_data")

        # Reset the sequence to start from 1
        cursor.execute("SELECT setval(pg_get_serial_sequence('graph_data', 'id'), 1, false)")

        # Insert the combined data into graph_data table and get the generated id
        insert_query = """
            INSERT INTO graph_data (location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        for row in combined_data:
            # Calculate tide_time_numeric
            tide_time_numeric = time_to_numeric(row[1])

            # Ensure the tide_time is formatted as 'HH:MM' and tide_date as 'YYYY-MM-DD'
            formatted_row = (
                row[0],  # location_id
                row[1].strftime('%H:%M') if isinstance(row[1], datetime.time) else row[1],  # tide_time
                tide_time_numeric,  # tide_time_numeric
                row[2],  # tide_height_mt
                row[3],  # tide_type
                row[4].strftime('%Y-%m-%d') if isinstance(row[4], datetime.date) else row[4]  # tide_date
            )

            cursor.execute(insert_query, formatted_row)

            # Fetch the id of the inserted row
            inserted_id = cursor.fetchone()[0]
            print(f"{inserted_id}, {tuple(formatted_row)}")

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error updating graph_data: {e}")
    finally:
        # Close the connection
        cursor.close()
        conn.close()

# Run the function to update graph_data
update_graph_data()
