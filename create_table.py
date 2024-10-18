# import psycopg2

# def insert_locations():
#     try:
#         # Connect to the database
#         conn = psycopg2.connect(
#             dbname="surf_forecast",
#             user="orlandosantos",
#             host="localhost",
#             port="5432"
#         )
#         cursor = conn.cursor()

#         # Example locations to insert
#         locations = [
#             {"name": "Location 1", "lat": 37.488897, "lng": -122.466919},
#             {"name": "Location 2", "lat": 34.0522, "lng": -118.2437},
#             {"name": "Location 3", "lat": 36.7783, "lng": -119.4179},
#             # Add more locations as needed
#         ]

#         # Insert each location into the database
#         for loc in locations:
#             cursor.execute('''
#                 INSERT INTO locations (location_name, latitude, longitude)
#                 VALUES (%s, %s, %s)
#             ''', (loc["name"], loc["lat"], loc["lng"]))

#         # Commit the changes to the database
#         conn.commit()
#         print("Locations inserted successfully!")

#     except Exception as e:
#         print(f"Error inserting locations: {e}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# if __name__ == "__main__":
#     insert_locations()
