from base_codes import connect
import pandas as pd

def transformation_cust_az12():

    with connect("test_database") as conn:

        df = pd.read_sql("SELECT * FROM ingestion.cust_az12", conn)
        print("Rows before cleaning:", len(df))

        df.columns = df.columns.str.strip()
        # Remove rows where CID is null
        df = df.dropna(subset=["CID"])
        # Clean CID text
        df["CID"] = df["CID"].astype(str).str.strip()
        # Remove empty CID rows
        df = df[df["CID"] != ""]
        # Remove duplicates by CID, keep last occurrence
        df = df.drop_duplicates(subset=["CID"], keep="last")

        # Customer key
        df["cst_key"] = df["CID"].str.replace("NAS", "", regex=False)

        # Customer ID
        df["cst_id"] = df["CID"].str.replace(r"\D", "", regex=True)
        df["cst_id"] = pd.to_numeric(df["cst_id"], errors="coerce")

        # Remove rows where extracted customer id is invalid
        df = df.dropna(subset=["cst_id"])
        df["cst_id"] = df["cst_id"].astype(int)
        df["BDATE"] = pd.to_datetime(df["BDATE"], errors="coerce").dt.date


        gender_map = {
            "M": "Male",
            "F": "Female",
            "MALE": "Male",
            "FEMALE": "Female"
        }

        df["GEN"] = (
            df["GEN"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(gender_map)
        )

        df = df.rename(columns={
            "BDATE": "cst_birthdate",
            "GEN": "cst_gndr"
        })

        # Keep only final columns
        df = df[["cst_id", "cst_key", "cst_birthdate", "cst_gndr"]]

        print("Rows after cleaning:", len(df))

        cur = conn.cursor()
        cur.execute("""
        TRUNCATE TABLE transformation.cust_az12
        """)

        df = df.astype(object).where(pd.notnull(df), None)

        rows = df.values.tolist()
        placeholders = ", ".join(["?"] * len(df.columns))

        insert_query = f"""
        INSERT INTO transformation.cust_az12
        VALUES ({placeholders})
        """

        cur.fast_executemany = True
        cur.executemany(insert_query, rows)

        conn.commit()

        print("cust_az12 transformed successfully!")


if __name__ == "__main__":
    transformation_cust_az12()