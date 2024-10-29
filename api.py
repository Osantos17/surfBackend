from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from datetime import datetime, time

app = Flask(__name__)
CORS(app)

def get_db_connection():
    conn = psycopg2.connect(
        dbname="surf_forecast",
        user="orlandosantos",
        host='localhost',
        port='5432'
    )
    return conn

def serialize_time(value):
    if isinstance(value, time):
        return value.strftime('%H:%M:%S') 
    return value

@app.route('/locations', defaults={'location_id': None}, methods=['GET'])
@app.route('/locations/<int:location_id>', methods=['GET'])
@app.route('/locations/tide-data', methods=['GET'])
@app.route('/locations/combined-data', methods=['GET'])
def get_data(location_id=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if location_id is None and request.path == '/locations':
            cursor.execute('SELECT id, location_name, latitude, longitude FROM locations ORDER BY id')
            locations = cursor.fetchall()
            
            locations_list = [
                {'id': loc[0], 'name': loc[1], 'latitude': loc[2], 'longitude': loc[3]}
                for loc in locations
            ]
            response = jsonify(locations_list)
        
        elif location_id is not None:
            cursor.execute('SELECT * FROM surf_data WHERE location_id = %s', (location_id,))
            surf_data = cursor.fetchall()
            
            surf_data_list = [
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
                    'swellDir': row[12],
                    'swellDir16Point': row[13],
                    'swellPeriod_secs': row[14],
                    'waterTemp_F': row[15]
                }
                for row in surf_data
            ]
            response = jsonify(surf_data_list)
        
        elif request.path == '/locations/tide-data':
            cursor.execute('SELECT * FROM tide_data')
            tide_data = cursor.fetchall()
            
            tide_data_list = [
                {
                    'id': row[0],
                    'location_id': row[1],
                    'tide_time': serialize_time(row[2]),
                    'tide_height': row[3],
                    'tide_type': row[4]
                }
                for row in tide_data
            ]
            response = jsonify(tide_data_list)

        elif request.path == '/locations/combined-data':
            cursor.execute('SELECT id, location_name, latitude, longitude FROM locations ORDER BY id')
            locations = cursor.fetchall()

            combined_data = []

            for loc in locations:
                loc_id = loc[0]
                loc_name = loc[1]
                loc_latitude = loc[2]
                loc_longitude = loc[3]

                # Fetch surf data for the current location
                cursor.execute('SELECT * FROM surf_data WHERE location_id = %s', (loc_id,))
                surf_data = cursor.fetchall()

                surf_data_list = [
                    {
                        'id': row[0],
                        'location_id': row[1],
                        'date': row[2],
                        'sunrise': row[3],
                        'sunset': row[4],
                        'time': row[5],
                        'tempF': row[6],
                        'windspeedMiles': row[7],
                        'winddirDegree': row[8],
                        'winddir16point': row[9],
                        'weatherDesc': row[10],
                        'swellHeight_ft': row[11],
                        'swellDir': row[12],
                        'swellDir16Point': row[13],
                        'swellPeriod_secs': row[14],
                        'waterTemp_F': row[15]
                    }
                    for row in surf_data
                ]

                # Fetch tide data for the current location
                cursor.execute('SELECT * FROM tide_data WHERE location_id = %s', (loc_id,))
                tide_data = cursor.fetchall()

                tide_data_list = [
                    {
                        'id': row[0],
                        'location_id': row[1],
                        'tide_time': serialize_time(row[2]),
                        'tide_height': row[3],
                        'tide_type': row[4],
                        'tide_date': (row[5]),
                    }
                    for row in tide_data
                ]

                # Append combined data for the current location
                combined_data.append({
                    'location_id': loc_id,
                    'location_name': loc_name,
                    'latitude': loc_latitude,
                    'longitude': loc_longitude,
                    'surf_data': surf_data_list,
                    'tide_data': tide_data_list
                })

            response = jsonify(combined_data)  # Add this line to ensure response is set

        cursor.close()
        conn.close()
        return response  # Return the response here
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
