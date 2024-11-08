# db.py
import psycopg2

def get_db_connection():
    # Your existing database connection function
    conn = psycopg2.connect(
        dbname="your_dbname", user="your_user", password="your_password", host="localhost", port="5432"
    )
    return conn

def move_last_tide_to_boundary(location_id: int) -> None:
    """Move the last tide entry of the previous day from tide_data to boundary_tide_data."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO boundary_tide_data (location_id, tide_time, tide_height_mt, tide_type, tide_date)
            SELECT location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM tide_data
            WHERE location_id = %s AND tide_date = CURRENT_DATE - INTERVAL '1 day'
            ORDER BY tide_time DESC LIMIT 1
            ON CONFLICT (location_id, tide_date, tide_time) DO NOTHING;
        ''', (location_id,))  # Ensure location_id is passed as part of the tuple

        conn.commit()
        print(f"Moved last tide entry to boundary_tide_data for location {location_id}.")

    except Exception as e:
        print(f"Error moving last tide entry for location {location_id}: {e}")
    finally:
        cursor.close()
        conn.close()
