from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from datetime import datetime,timedelta, time as time_type
import time 

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )

# Utility functions to serialize date and time
def serialize_time(value):
    if isinstance(value, datetime):
        return value.strftime('%H:%M')
    elif isinstance(value, time_type):  # Use time_type to avoid conflict
        return value.strftime('%H:%M')
    elif isinstance(value, str):
        try:
            time_obj = datetime.strptime(value, '%H:%M')
            return time_obj.strftime('%H:%M')
        except ValueError:
            try:
                time_obj = datetime.strptime(value, '%I:%M %p')
                return time_obj.strftime('%H:%M')
            except ValueError:
                return value
    return value

def serialize_date(value):
    return value.strftime('%a %b %d') if isinstance(value, datetime) else value

@app.route('/locations', methods=['GET'])
def get_locations():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id, location_name, latitude, longitude FROM locations')
    locations = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify([
        {
            'id': row[0],
            'location_name': row[1],
            'latitude': row[2],
            'longitude': row[3]
        } for row in locations
    ])

@app.route('/locations/combined-data/<int:location_id>', methods=['GET'])
def get_combined_data_by_id(location_id):
    """Fetches combined surf and tide data for a specific location."""
    conn, cursor = None, None
    try:
        include_surf = request.args.get('include_surf', 'true').lower() == 'true'
        include_tide = request.args.get('include_tide', 'true').lower() == 'true'

        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch location data
        cursor.execute('''
            SELECT id, location_name, latitude, longitude
            FROM locations WHERE id = %s
        ''', (location_id,))
        location = cursor.fetchone()

        if not location:
            return jsonify({'error': 'Location not found'}), 404

        combined_data = {
            'location_id': location[0],
            'location_name': location[1],
            'latitude': location[2],
            'longitude': location[3],
        }

        # Fetch and attach surf data
        if include_surf:
            cursor.execute('''
                SELECT id, location_id, date, sunrise, sunset, time, tempF, windspeedMiles, winddirDegree, 
                       winddir16point, weatherDesc, swellHeight_ft, swelldir, swelldir16point, swellperiod_secs
                FROM surf_data WHERE location_id = %s
            ''', (location_id,))
            surf_data = cursor.fetchall()
            combined_data['surf_data'] = [
                {
                    'id': row[0],
                    'location_id': row[1],
                    'date': serialize_date(row[2]),
                    'sunrise': serialize_time(row[3]),
                    'sunset': serialize_time(row[4]),
                    'time': serialize_time(row[5]),
                    'tempF': row[6],
                    'windspeedMiles': row[7],
                    'winddirDegree': row[8],
                    'winddir16point': row[9],
                    'weatherDesc': row[10],
                    'swellHeight_ft': row[11],
                    'swelldir': row[12],
                    'swelldir16point': row[13],
                    'swellperiod_secs': row[14],
                }
                for row in surf_data
            ]

        # Fetch and attach tide data
        if include_tide:
            cursor.execute('''
                SELECT id, location_id, tide_time, tide_height_mt, tide_type, tide_date
                FROM tide_data WHERE location_id = %s
            ''', (location_id,))
            tide_data = cursor.fetchall()
            combined_data['tide_data'] = [
                {
                    'id': row[0],
                    'location_id': row[1],
                    'tide_time': serialize_time(row[2]),
                    'tide_height': row[3],
                    'tide_type': row[4],
                    'tide_date': serialize_date(row[5])
                }
                for row in tide_data
            ]

        return jsonify(combined_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@app.route('/api/combined-tide-data/<int:location_id>', methods=['GET'])
def get_combined_tide_data(location_id):
    """Fetches combined tide data from both tide_data and boundary_tide_data for a given location."""
    conn, cursor = None, None
    try:
        today = datetime.now()
        three_days_ago = today - timedelta(days=1)
        two_days_after = today + timedelta(days=2)

        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch tide data for the date range from tide_data table
        cursor.execute('''
            SELECT id, location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM tide_data
            WHERE location_id = %s AND tide_date BETWEEN %s AND %s
        ''', (location_id, three_days_ago.date(), two_days_after.date()))
        
        tide_data = cursor.fetchall()
        print(f"Tide Data: {tide_data}")  # Add this debug statement

        # Fetch tide data from boundary_tide_data table for the same date range
        cursor.execute('''
            SELECT id, location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM boundary_tide_data
            WHERE location_id = %s AND tide_date BETWEEN %s AND %s
        ''', (location_id, three_days_ago.date(), two_days_after.date()))
        
        boundary_tide_data = cursor.fetchall()
        print(f"Boundary Tide Data: {boundary_tide_data}")  # Add this debug statement

        # Format and combine tide data for JSON serialization
        combined_data = [
            {
                'id': row[0],
                'location_id': row[1],
                'tide_time': serialize_time(row[2]),
                'tide_height_mt': row[3],
                'tide_type': row[4],
                'tide_date': serialize_date(row[5])
            } for row in tide_data + boundary_tide_data
        ]

        return jsonify(combined_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        
@app.route('/graph-data/<int:location_id>', methods=['GET'])
def get_graph_data(location_id):
    """Fetches graph data for a specific location."""
    conn, cursor = None, None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch graph data based on location_id
        cursor.execute('''
            SELECT id, location_id, tide_time, tide_height_mt, tide_type, tide_date
            FROM graph_data WHERE location_id = %s
        ''', (location_id,))
        graph_data = cursor.fetchall()

        if not graph_data:
            return jsonify({'error': 'No graph data found for this location'}), 404

        # Format and serialize graph data for JSON response
        graph_data_response = [
            {
                'Time': serialize_time(row[2]),  # Formatting tide time (e.g., '22:01')
                'Tide Height': row[3],  # Tide height in meters
                'Tide Type': row[4],  # Tide type (e.g., 'LOW' or 'HIGH')
                'Date': row[5].strftime('%Y-%m-%d') if isinstance(row[5], datetime) else row[5].strftime('%Y-%m-%d')
            }
            for row in graph_data
        ]

        return jsonify(graph_data_response)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor: cursor.close()
        if conn: conn.close()


if __name__ == "__main__":
    app.run(debug=True)
