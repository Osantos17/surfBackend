import psycopg2
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv('config.env')

def get_db_connection():
    DATABASE_URL = os.environ.get('DATABASE_URL')

    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        conn = psycopg2.connect(
            dbname="surf_forecast",
            user="your_local_user",  
            host="localhost",
            port="5432"
        )
    
    return conn


def create_db() -> None:
    """Create the database and necessary tables."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id SERIAL PRIMARY KEY,
                location_name VARCHAR(100) NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                preferred_wind_dir_min INTEGER,
                preferred_wind_dir_max INTEGER,
                preferred_swell_dir_min INTEGER,
                preferred_swell_dir_max INTEGER,
                bad_swell_dir_min INTEGER,
                bad_swell_dir_max INTEGER,
                wavecalc NUMERIC,
                region VARCHAR(50),
                reef BOOLEAN  -- Add the new 'reef' column here
            )
        ''')
        

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS surf_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                date VARCHAR(20) NOT NULL,
                sunrise VARCHAR(10),
                sunset VARCHAR(10),
                time VARCHAR(10),
                tempF FLOAT,
                windspeedMiles FLOAT,
                winddirDegree INT,
                winddir16Point VARCHAR(10),
                weatherDesc VARCHAR(100),
                swellHeight_ft FLOAT,
                swellDir INT,
                swellDir16Point VARCHAR(10),
                swellPeriod_secs FLOAT,
                waterTemp_F FLOAT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_time VARCHAR(10),
                tide_height_mt FLOAT,
                tide_type VARCHAR(10),
                tide_date DATE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS graph_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_date DATE,
                tide_height_mt DOUBLE PRECISION,
                tide_time TIME WITHOUT TIME ZONE,
                tide_time_numeric INTEGER,
                tide_type VARCHAR(50)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS graph_points (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                graph_date DATE NOT NULL,
                graph_time TIME WITHOUT TIME ZONE NOT NULL,
                tide_height NUMERIC,
                tide_type VARCHAR(20)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS boundary_tide_data (
                id SERIAL PRIMARY KEY,
                location_id INT REFERENCES locations(id),
                tide_height_mt DOUBLE PRECISION,
                tide_date DATE,
                tide_time VARCHAR(20),
                tide_type VARCHAR(20)
            )
        ''')

        conn.commit()
        print("Database and tables created successfully!")

    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()
        
    
        
        
def add_columns_to_locations_table() -> None:
    """Add missing columns to the locations table."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Add new columns to the locations table
        cursor.execute('''
            ALTER TABLE locations
            ADD COLUMN IF NOT EXISTS preferred_wind_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_wind_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_swell_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS preferred_swell_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS bad_swell_dir_min INTEGER,
            ADD COLUMN IF NOT EXISTS bad_swell_dir_max INTEGER,
            ADD COLUMN IF NOT EXISTS wavecalc VARCHAR(100),
            ADD COLUMN IF NOT EXISTS Region VARCHAR(50),
            ADD COLUMN IF NOT EXISTS reef BOOLEAN;  -- Add the new 'reef' column here
        ''')

        conn.commit()
        print("Columns added successfully!")

    except Exception as e:
        print(f"Error adding columns: {e}")
    finally:
        cursor.close()
        conn.close()