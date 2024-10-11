import pandas as pd
import mysql.connector
import csv

# Създаване на масива с кодовете на държавите в Европа
european_country_codes = [
    "BE", "EL", "LT", "PT", "BG", "ES", "LU", "CZ", "FR", "HU", "SI",
    "DK", "HR", "MT", "SK", "DE", "IT", "NL", "FI", "EE", "CY", "AT",
    "SE", "IE", "LV", "PL", "IS", "NO", "LI", "CH", "BA", "ME", "MD",
    "MK", "GE", "AL", "RS", "TR", "UA"
]

# Свързване към MySQL база данни
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="eu_postal_codes"
)

mycursor = mydb.cursor()

# Прочитане на текстовия файл и филтриране на редовете
with open('allCountriesCSV.csv', mode='r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        if row['COUNTRY'] in european_country_codes:
            sql = """
            INSERT INTO cities (country, postal_code, city, state, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            val = (
                row['COUNTRY'], row['POSTAL_CODE'], row['CITY'], row['STATE'],
                row['LATITUDE'], row['LONGITUDE']
            )
            mycursor.execute(sql, val)

# Записване на промените в базата данни
mydb.commit()

print(mycursor.rowcount, "record(s) inserted.")


# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     print(european_country_codes)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
