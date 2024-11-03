from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from datetime import datetime, time

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )

def serialize_time(value):
    if isinstance(value, time):
        return value.strftime('%H:%M')
    return value

def serialize_time_12hr(value):
    if isinstance(value, time):
        return value.strftime('%I %p').lstrip('0')
    return value

def serialize_date(value):
    if isinstance(value, datetime):
        return value.strftime('%a %b %d')
    return value

@app.route('/locations', methods=['GET'])
def get_locations():
    """Fetches all locations."""
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
    """Fetches combined surf and tide data for a given location ID."""
    conn = None
    cursor = None  # Initialize cursor variable
    try:
        include_surf = request.args.get('include_surf', 'true').lower() == 'true'
        include_tide = request.args.get('include_tide', 'true').lower() == 'true'

        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch location data
        cursor.execute('''
            SELECT loc.id, loc.location_name, loc.latitude, loc.longitude
            FROM locations loc
            WHERE loc.id = %s
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

        # Fetch surf data if requested
        if include_surf:
            cursor.execute('''
                SELECT id, location_id, date, sunrise, sunset, time, tempF, windspeedMiles, winddirDegree, winddir16point, weatherDesc, swellHeight_ft, swelldir, swelldir16point, swellperiod_secs
                FROM surf_data WHERE location_id = %s
            ''', (location_id,))
            surf_data = cursor.fetchall()
            combined_data['surf_data'] = [
                {
                    'id': row[0],
                    'location_id': row[1],
                    'date': row[2],
                    'sunrise': serialize_time(row[3]),
                    'sunset': serialize_time(row[4]),
                    'time': serialize_time(row[5]),
                    'tempF': row[6],
                    'windspeedMiles': row[7],
                    'winddirDegree': row[8],
                    'winddir16point': row[9],
                    'weatherDesc': row[10],
                    'swellHeight_ft': row[11],
                    'swelldir' :row[12],
                    'swelldir16point' :row[13],
                    'swellperiod_secs' :row[14],
                }
                for row in surf_data
            ]

        # Fetch tide data if requested
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
                    'tide_date': row[5]
                }
                for row in tide_data
            ]

        return jsonify(combined_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        # Ensure cursor and connection are closed properly
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            
if __name__ == "__main__":
    app.run(debug=True)
