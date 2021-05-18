#!/usr/bin/python
import sqlite3
import csv
import matplotlib.pyplot as plt
import pandas as pd

conn = sqlite3.connect("summit_media.db")
cur = conn.cursor()
fd = open('D:/sqlite/gui/sqlitestudio-3.3.3/SQLiteStudio/query.sql', 'r')
sql_file = fd.read()
fd.close()
cur.execute(sql_file)

with open("final_dataset.csv", "w", newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cur.description])
    csv_writer.writerows(cur)
csv_file.close()

df = pd.read_csv('final_dataset.csv')
revenue = df['revenue']
pfy = df['preferred_film_year']
plt.pie(revenue, labels=pfy)
plt.show()