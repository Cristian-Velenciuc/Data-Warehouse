import pandas as pd
import os
import sys
from pathlib import Path
# add project root to Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

export_folder = "./data_source"
os.makedirs(export_folder, exist_ok=True)

from base_codes import connect
with connect("test_database") as conn:

    tables = pd.read_sql("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'transformation'
    """, conn)

    for table_name in tables["TABLE_NAME"]:

        df = pd.read_sql(f"SELECT * FROM transformation.{table_name}", conn)

        file_path = os.path.join(export_folder, f"{table_name}.xlsx")

        df.to_excel(file_path, index=False)

        os.startfile(file_path)

        print(f"Opened {table_name}.xlsx")