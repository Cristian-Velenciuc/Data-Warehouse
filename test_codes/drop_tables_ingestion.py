import sys
from pathlib import Path

# add project root to Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

tables = [
    "cust_info",
    "prd_info",
    "sales_details",
    "cust_az12",
    "loc_a101",
    "px_cat_g1v2"
]

from base_codes import connect
with connect("test_database") as conn:
    cur = conn.cursor()

    for table in tables:
        cur.execute(f"""
        IF OBJECT_ID('ingestion.{table}', 'U') IS NOT NULL
        DROP TABLE ingestion.{table}
        """)
        print(f"Dropped ingestion.{table}")

    conn.commit()