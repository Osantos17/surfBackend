from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2

app = Flask(__name__)
CORS(app)

def get_db_connection():
  conn = psycopg2.connect(
    dbname = "surf_forecast",
    user = "orlandosantos",
    host='localhost',
    port='5432'
  )
  return conn

@app.route('/locations', methods=['GET'])
def get_locations():
  try:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id, location_name, latitude, longitude FROM locations')
    locations = cursor.fetchall()
    
    locations_lists = [
      {'id': loc[0], 'name': loc[1], 'latitude': loc[2], 'longitude': loc[3]}
      for loc in locations
    ]
    
    cursor.close()
    conn.close()
    return jsonify(locations_lists)
  
  except Exception as e:
    return jsonify({'error': str(e)}), 500
  

def get_surf_data(location_id):
  try:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM surf_data WHERE location_id = %s', (location_id,))
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
    
    cursor.close()
    conn.close()
    return jsonify(surf_data_list)
  
  except Exception as e:
    return jsonify({'error': str(e)}), 500
  
if __name__ == "__main__":
  app.run(debug=True)
    