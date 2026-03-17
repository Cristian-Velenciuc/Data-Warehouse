from base_codes import connect
import pandas as pd


def transformation_loc_a101():

    with connect("test_database") as conn:

        df = pd.read_sql("SELECT * FROM ingestion.loc_a101", conn)
        print("Rows before cleaning:", len(df))
        df.columns = df.columns.str.strip()

        # Remove rows where CID is null
        df = df.dropna(subset=["CID"])

        # Clean text fields
        df["CID"] = df["CID"].astype(str).str.strip()
        df["CNTRY"] = df["CNTRY"].astype(str).str.strip()

        # Remove empty CID rows
        df = df[df["CID"] != ""]

        # Remove duplicate customers, keep last
        df = df.drop_duplicates(subset=["CID"], keep="last")
        df["CNTRY"] = df["CNTRY"].replace({
            "": None,
            "NULL": None,
            "null": None,
            "None": None
        })

        df["CNTRY"] = df["CNTRY"].fillna("NA")

        country_map = {
            "US": "United States",
            "USA": "United States",
            "UNITED STATES": "United States",
            "U.S.": "United States",
            "U.S.A.": "United States",
            "DE": "Germany",
            "GERMANY": "Germany",
            "UNITED KINGDOM": "United Kingdom",
            "UK": "United Kingdom",
            "FRANCE": "France",
            "CANADA": "Canada",
            "AUSTRALIA": "Australia",
            "NA": "NA"
        }

        df["CNTRY"] = (
            df["CNTRY"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(country_map)
            .fillna(df["CNTRY"].astype(str).str.strip().str.title())
        )

        print("Rows after cleaning:", len(df))
        cur = conn.cursor()
        cur.execute("""
        IF OBJECT_ID('transformation.loc_a101', 'U') IS NULL
        BEGIN
            CREATE TABLE transformation.loc_a101 (
                CID VARCHAR(50),
                CNTRY VARCHAR(50)
            )
        END
        """)
        # Truncate before load
        cur.execute("""
        TRUNCATE TABLE transformation.loc_a101
        """)

        # Keep only final columns
        df = df[["CID", "CNTRY"]]

        df = df.astype(object).where(pd.notnull(df), None)
        rows = df.values.tolist()

        insert_query = """
        INSERT INTO transformation.loc_a101
        (CID, CNTRY)
        VALUES (?, ?)
        """

        cur.fast_executemany = True
        cur.executemany(insert_query, rows)
        conn.commit()

        print("loc_a101 transformed successfully!")


if __name__ == "__main__":
    transformation_loc_a101()