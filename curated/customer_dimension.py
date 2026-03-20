import pandas as pd
from base_codes import connect


def customer_dimension():
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF OBJECT_ID('curated.dim_customer', 'U') IS NULL
        CREATE TABLE curated.dim_customer (
            customer_sk INT IDENTITY(1,1) PRIMARY KEY,
            customer_id INT,
            customer_key VARCHAR(50),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            marital_status VARCHAR(20),
            gender VARCHAR(10),
            birthdate DATE,
            country VARCHAR(100),
            create_date DATE
        )
        """)
        conn.commit()

        customer_info = pd.read_sql("""
            SELECT *
            FROM transformation.cust_info
        """, conn)

        customer_az12 = pd.read_sql("""
            SELECT cst_key, cst_birthdate
            FROM transformation.cust_az12
        """, conn)

        customer_location = pd.read_sql("""
            SELECT CID, CNTRY
            FROM transformation.loc_a101
        """, conn)

        customer_df = customer_info.merge(
            customer_az12,
            how="left",
            on="cst_key"
        )

        customer_df = customer_df.merge(
            customer_location,
            how="left",
            left_on="cst_key",
            right_on="CID"
        )

        customer_df = customer_df[[
            "cst_id",
            "cst_key",
            "cst_firstname",
            "cst_lastname",
            "cst_marital_status",
            "cst_gndr",
            "cst_birthdate",
            "CNTRY",
            "cst_create_date"
        ]].copy()

        customer_df = customer_df.rename(columns={
            "cst_id": "customer_id",
            "cst_key": "customer_key",
            "cst_firstname": "first_name",
            "cst_lastname": "last_name",
            "cst_marital_status": "marital_status",
            "cst_gndr": "gender",
            "cst_birthdate": "birthdate",
            "CNTRY": "country",
            "cst_create_date": "create_date"
        })

        customer_df["birthdate"] = pd.to_datetime(customer_df["birthdate"], errors="coerce").dt.date
        customer_df["create_date"] = pd.to_datetime(customer_df["create_date"], errors="coerce").dt.date

        customer_df = customer_df.where(pd.notna(customer_df), None)

        cur.execute("TRUNCATE TABLE curated.dim_customer")
        conn.commit()

        rows = [
            (
                int(row["customer_id"]) if row["customer_id"] is not None else None,
                str(row["customer_key"]) if row["customer_key"] is not None else None,
                str(row["first_name"]) if row["first_name"] is not None else None,
                str(row["last_name"]) if row["last_name"] is not None else None,
                str(row["marital_status"]) if row["marital_status"] is not None else None,
                str(row["gender"]) if row["gender"] is not None else None,
                row["birthdate"],
                str(row["country"]) if row["country"] is not None else None,
                row["create_date"]
            )
            for _, row in customer_df.iterrows()
        ]

        cur.executemany("""
            INSERT INTO curated.dim_customer (
                customer_id,
                customer_key,
                first_name,
                last_name,
                marital_status,
                gender,
                birthdate,
                country,
                create_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()

        return customer_df