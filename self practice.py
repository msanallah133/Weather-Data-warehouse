import pyodbc
import pandas as pd

conn_string = 'Driver={SQL Server};Server=SUNNY;Database=weather;Trusted_Connection=yes;'

conn = pyodbc.connect(conn_string)
cursor = conn.cursor()
csv_file = 'weather.csv'
df = pd.read_csv(csv_file)

table_name = 'WeatherData'

for index , row in df.iterrows():
    columns = ', '.join([f'[{col}]' for col in row.index])
    # columns = ','.join([f' [{col}]]' for col in row.index])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['?' for _ in row])})"
    cursor.execute(insert_query, tuple(row))
    conn.commit()
    
cursor.close()
conn.close()

                    
                    