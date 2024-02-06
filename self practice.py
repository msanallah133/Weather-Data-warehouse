import pyodbc
import pandas as pd

conn_string = 'Driver={SQL Server};Server=SUNNY;Database=weather;Trusted_Connection=yes;'

conn = pyodbc.connect(conn_string)
cursor = conn.cursor()
csv1_file = 'weather.csv'
df1 = pd.read_csv(csv1_file)
csv2_file='weather_data.csv'
df2 = pd.read_csv(csv2_file)

table1_name = 'WeatherData'
table2_name = 'weather_Data'

for index , row in df1.iterrows():
    columns = ', '.join([f'[{col}]' for col in row.index])
    insert1_query = f"INSERT INTO {table1_name} ({columns}) VALUES ({', '.join(['?' for _ in row])})"
    cursor.execute(insert1_query, tuple(row))
    conn.commit()
    
for index, row in df2.iterrows():
    # Construct the INSERT query
    insert2_query = f"""
    INSERT INTO {table2_name} (
        [ODT_C], [ORH_P], [DSR_W_m2_1], [DSR_W_m2_2],
        [6h_P_ODT_C], [12h_P_ODT_C], [24h_P_ODT_C],
        [6h_P_ORH_P], [12h_P_ORH_P], [24h_P_ORH_P],
        [6h_P_DSR_W_m2_1], [12h_P_DSR_W_m2_2], [24h_P_DSR_W_m2_3],
        [6h_P_DSR_W_m2_4], [12h_P_DSR_W_m2_5], [24h_P_DSR_W_m2_6]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Execute the INSERT query with row values
    cursor.execute(insert2_query, tuple(row))

    # Commit the transaction (optional, depending on your needs)
    conn.commit()

# Close the cursor and connection

cursor.close()
conn.close()

                    
                    