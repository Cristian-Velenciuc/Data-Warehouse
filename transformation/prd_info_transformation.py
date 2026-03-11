from base_codes import connect
import pandas as pd

def transformation_prd_info():

    with connect("test_database") as conn:

        df = pd.read_sql("SELECT * FROM ingestion.prd_info", conn)
        print("Rows before cleaning:", len(df))
        print(df)

        df["prd_cat"] = df["prd_key"].str[:5] # First 5 become new column (category)    
        df["prd_key"] = df["prd_key"].str[6:] # FIrst 5 + the(-) element get removed from this column

        df = df[
                    [
                        "prd_id",
                        "prd_key",
                        "prd_cat",
                        "prd_nm",
                        "prd_cost",
                        "prd_line",
                        "prd_start_dt",
                        "prd_end_dt",
                    ]
                ]
        
        df["prd_cat"] = df["prd_cat"].str.replace("-", "_", regex=False) # Replace - with _

        df["prd_cost"] = df["prd_cost"].fillna(0) # Replace rows where cost is NULL with cost = 0

        # Replace letters with actual lines
        df["prd_line"] = df["prd_line"].str.strip().str.upper()
        line_map = {
            "M": "Mountain",
            "R": "Road",
            "T": "Touring",
            "S": "Sport"
        }
        df["prd_line"] = df["prd_line"].map(line_map).fillna(df["prd_line"])
        print("Rows after cleaning:", len(df))

        # Fix the time issue
        df["prd_end_dt"] = df.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)

        # Truncate Table
        cur = conn.cursor()
        cur.execute("""
        TRUNCATE TABLE transformation.prd_info
        """)

        df = df.astype(object).where(pd.notnull(df), None)

        rows = df.values.tolist()

        placeholders = ", ".join(["?"] * len(df.columns))

        insert_query = f"""
        INSERT INTO transformation.prd_info
        VALUES ({placeholders})
        """

        cur.fast_executemany = True
        cur.executemany(insert_query, rows)

        conn.commit()

        print("Customer table transformed successfully!")


if __name__ == "__main__":
    transformation_prd_info()