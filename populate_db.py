from db import DB
import pandas as pd
import os

db = DB(host="localhost", port=5433, database="postgres", user="postgres", password="postgres")

for file in os.listdir("silver"):
    if not file.endswith(".parquet"):
        continue

    df = pd.read_parquet(f"silver/{file}")
    table = file.replace(".parquet", "")

    db.create_table(table, df)
    db.insert_data(table, df)

db.close()