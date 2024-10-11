import mysql.connector
import pandas as pd

connection =  mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="eu_postal_codes",
)

try:
    query = "SELECT * FROM cities"

    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)

    columns = [i[0] for i in cursor.description]

    with open('output_data.csv', 'w', encoding='utf-8') as f:
        f.write(','.join(columns) + '\n')

        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break

            for row in rows:
                f.write(','.join(str(row[col]) for col in columns) + '\n')

finally:
    connection.close()

print("Данните са успешно извлечени и записани във файл.")
