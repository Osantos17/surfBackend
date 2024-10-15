import psycopg2
from psycopg2 import sql

def create_database(surf_locations):
  conn = psycopg2.connect(
    dbname='postgres',
    user='orlandosantos',
    host='localhost',
    port='5432'
  )
  
  cursor = conn.cursor()
  
  create_table_query = '''
  CREATE TABLE IF NOT EXISTS surf_data (
    id SERIAL PRIMARY KEY,
    
  )'''
  
  cursor.execute(sql.SQL('CREATE DATAVASE {}').fornat(sql.Identifier(surf_locations)))
  
  cursor.close()
  conn.close()
  
create_database('surf_database')