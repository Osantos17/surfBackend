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

def time_to_numeric(tide_time):
    """Convert time to numeric value representing minutes since midnight."""
    # If tide_time is a string, convert it to a time object
    if isinstance(tide_time, str):
        # Try to convert string time (e.g. "22:01") to a datetime.time object
        try:
            tide_time = datetime.datetime.strptime(tide_time, "%H:%M").time()
        except ValueError:
            return 0  # Return 0 if format is not as expected

    # Ensure the time is a valid datetime.time object
    if isinstance(tide_time, datetime.time):
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

            # Convert tide_time to string for display
            tide_time_str = str(row[1]) if isinstance(row[1], datetime.time) else row[1]

            formatted_row = (
                row[0],  # location_id
                tide_time_str,  # tide_time (as string)
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

