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

        customer_info = pd.read_sql("SELECT * FROM transformation.cust_info", conn)
        customer_az12 = pd.read_sql("SELECT cst_key, cst_birthdate FROM transformation.cust_az12", conn)
        customer_location = pd.read_sql("SELECT * FROM transformation.loc_a101", conn)

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

        customer_df["birthdate"] = pd.to_datetime(customer_df["birthdate"], errors="coerce")
        customer_df["create_date"] = pd.to_datetime(customer_df["create_date"], errors="coerce")

        cur.execute("TRUNCATE TABLE curated.dim_customer")
        conn.commit()

        for _, row in customer_df.iterrows():
            customer_id = None if pd.isna(row["customer_id"]) else int(row["customer_id"])
            customer_key = None if pd.isna(row["customer_key"]) else str(row["customer_key"])
            first_name = None if pd.isna(row["first_name"]) else str(row["first_name"])
            last_name = None if pd.isna(row["last_name"]) else str(row["last_name"])
            marital_status = None if pd.isna(row["marital_status"]) else str(row["marital_status"])
            gender = None if pd.isna(row["gender"]) else str(row["gender"])
            country = None if pd.isna(row["country"]) else str(row["country"])

            birthdate = None if pd.isna(row["birthdate"]) else row["birthdate"].date()
            create_date = None if pd.isna(row["create_date"]) else row["create_date"].date()

            cur.execute("""
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
            """,
            customer_id,
            customer_key,
            first_name,
            last_name,
            marital_status,
            gender,
            birthdate,
            country,
            create_date)

        conn.commit()

        return customer_df