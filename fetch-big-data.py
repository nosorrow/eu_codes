from sqlalchemy import create_engine
import pandas as pd

db_host = 'localhost'
db_user = 'root'
db_password = ''
db_name = 'eu_postal_codes'

connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_string)

query = "SELECT * FROM cities"
chunk_size = 10000

chunks = pd.read_sql(query, engine, chunksize=chunk_size)

with open('output_data.csv', 'w', encoding='utf-8', newline='') as f:
    first_chunk = next(chunks)
    first_chunk.to_csv(f, header=True, index=False)

    for chunk in chunks:
        chunk.to_csv(f, header=False, index=False)

print("Данните са успешно извлечени и записани във файл.")
