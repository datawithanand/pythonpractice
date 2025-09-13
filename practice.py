import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

csv_file = "/Users/anandsuresh/Downloads/Clever Studies/vehicle_reg.csv"

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="vehicle_db",
    user="anandsuresh",
    host="localhost"
)
cur = conn.cursor()

chunksize = 100000
total_rows = 0

for chunk in pd.read_csv(
    csv_file,
    chunksize=chunksize,
    sep='\t',
    engine='python',
    on_bad_lines='skip',
    dtype=str
):
    # Convert numeric columns
    chunk['VehicleCost'] = pd.to_numeric(chunk['VehicleCost'], errors='coerce')
    chunk['OdometerReading'] = pd.to_numeric(chunk['OdometerReading'], errors='coerce')

    # Auto-create table on first chunk
    if total_rows == 0:
        columns = ', '.join(f'"{col}" TEXT' for col in chunk.columns)
        create_table_query = f'CREATE TABLE IF NOT EXISTS vehicle_reg ({columns});'
        cur.execute(create_table_query)
        conn.commit()

    # Insert chunk using execute_values (fast)
    records = chunk.values.tolist()
    cols = ','.join(f'"{col}"' for col in chunk.columns)
    sql = f'INSERT INTO vehicle_reg ({cols}) VALUES %s'
    execute_values(cur, sql, records)
    conn.commit()

    total_rows += len(chunk)
    print(f"Loaded {total_rows} rows...")

cur.close()
conn.close()
print("All data successfully loaded into PostgreSQL!")
