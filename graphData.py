import os
import psycopg2
import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv


if os.getenv('ENV') != 'production':
    load_dotenv('config.env')

def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        url_parts = urlparse(db_url)
        return psycopg2.connect(
            database=url_parts.path[1:],
            user=url_parts.username,
            password=url_parts.password,
            host=url_parts.hostname,
            port=url_parts.port
        )
    else:
        raise Exception("DATABASE_URL not set in production")


def time_to_numeric(tide_time):
    """Convert time to numeric value representing minutes since midnight."""
    if isinstance(tide_time, str):
        # Try parsing the time string in multiple formats
        for time_format in ['%H:%M', '%H:%M:%S']:
            try:
                time_obj = datetime.datetime.strptime(tide_time, time_format).time()
                return time_obj.hour * 60 + time_obj.minute
            except ValueError:
                continue
        print(f"Invalid time format: {tide_time}")
        return 0  # Default value for invalid time formats
    elif isinstance(tide_time, datetime.time):
        return tide_time.hour * 60 + tide_time.minute
    return 0




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

        cursor.execute("""
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM boundary_tide_data
            ORDER BY location_id, tide_date, tide_time
        """)
        boundary_data_rows = cursor.fetchall()

        combined_data = tide_data_rows + boundary_data_rows
        combined_data.sort(key=lambda x: (x[0], x[4], time_to_numeric(x[1])))

        cursor.execute("DELETE FROM graph_data")

        cursor.execute("SELECT setval(pg_get_serial_sequence('graph_data', 'id'), 1, false)")

        insert_query = """
            INSERT INTO graph_data (location_id, tide_time, tide_time_numeric, tide_height_mt, tide_type, tide_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        for row in combined_data:
            
            tide_time_numeric = time_to_numeric(row[1])

            formatted_row = (
                row[0],  
                row[1].strftime('%H:%M') if isinstance(row[1], datetime.time) else row[1], 
                tide_time_numeric, 
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
