from base_codes import connect
import pandas as pd


def transformation_cust_info():

    with connect("test_database") as conn:

        df = pd.read_sql("SELECT * FROM ingestion.cust_info", conn)
        print("Rows before cleaning:", len(df))

    
        # Remove rows where customer id is null
        df = df.dropna(subset=["cst_id"])

        # Remove duplicate customers
        df = df.drop_duplicates(subset="cst_id", keep="last")

        # Standardize marital status
        marital_map = {
            "S": "Single",
            "M": "Married"
        }

        df["cst_marital_status"] = df["cst_marital_status"].map(marital_map).fillna(df["cst_marital_status"])

        # Standardize gender
        gender_map = {
            "M": "Male",
            "F": "Female"
        }

        df["cst_gndr"] = df["cst_gndr"].map(gender_map).fillna(df["cst_gndr"])
        print("Rows after cleaning:", len(df))

        # Truncate Table
        cur = conn.cursor()
        cur.execute("""
        TRUNCATE TABLE transformation.cust_info
        """)

        df = df.astype(object).where(pd.notnull(df), None)

        rows = df.values.tolist()

        placeholders = ", ".join(["?"] * len(df.columns))

        insert_query = f"""
        INSERT INTO transformation.cust_info
        VALUES ({placeholders})
        """

        cur.fast_executemany = True
        cur.executemany(insert_query, rows)

        conn.commit()

        print("Customer table transformed successfully!")


if __name__ == "__main__":
    transformation_cust_info()