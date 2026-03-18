import pandas as pd
from base_codes import connect


def product_dimension():
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF OBJECT_ID('curated.dim_product', 'U') IS NULL
        CREATE TABLE curated.dim_product (
            product_sk INT IDENTITY(1,1) PRIMARY KEY,
            product_id INT,
            product_key VARCHAR(50),
            product_name VARCHAR(150),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            maintenance VARCHAR(10),
            product_line VARCHAR(50),
            product_cost DECIMAL(10,2),
            start_date DATE,
            end_date DATE
        )
        """)
        conn.commit()

        prd_info = pd.read_sql("SELECT * FROM transformation.prd_info", conn)
        px_cat = pd.read_sql("SELECT * FROM transformation.px_cat_g1v2", conn)

        product_df = prd_info.merge(
            px_cat,
            how="left",
            left_on="prd_key",
            right_on="ID"
        )

        product_df = product_df[[
            "prd_id",
            "prd_key",
            "prd_nm",
            "CAT",
            "SUBCAT",
            "MAINTENANCE",
            "prd_line",
            "prd_cost",
            "prd_start_dt",
            "prd_end_dt"
        ]].copy()

        product_df = product_df.rename(columns={
            "prd_id": "product_id",
            "prd_key": "product_key",
            "prd_nm": "product_name",
            "CAT": "category",
            "SUBCAT": "subcategory",
            "MAINTENANCE": "maintenance",
            "prd_line": "product_line",
            "prd_cost": "product_cost",
            "prd_start_dt": "start_date",
            "prd_end_dt": "end_date"
        })

        product_df["start_date"] = pd.to_datetime(product_df["start_date"], errors="coerce")
        product_df["end_date"] = pd.to_datetime(product_df["end_date"], errors="coerce")
        product_df["product_cost"] = pd.to_numeric(product_df["product_cost"], errors="coerce")

        cur.execute("TRUNCATE TABLE curated.dim_product")
        conn.commit()

        for _, row in product_df.iterrows():
            product_id = None if pd.isna(row["product_id"]) else int(row["product_id"])
            product_key = None if pd.isna(row["product_key"]) else str(row["product_key"])
            product_name = None if pd.isna(row["product_name"]) else str(row["product_name"])
            category = None if pd.isna(row["category"]) else str(row["category"])
            subcategory = None if pd.isna(row["subcategory"]) else str(row["subcategory"])
            maintenance = None if pd.isna(row["maintenance"]) else str(row["maintenance"])
            product_line = None if pd.isna(row["product_line"]) else str(row["product_line"])

            product_cost = None if pd.isna(row["product_cost"]) else round(float(row["product_cost"]), 2)

            start_date = None if pd.isna(row["start_date"]) else row["start_date"].date()
            end_date = None if pd.isna(row["end_date"]) else row["end_date"].date()

            cur.execute("""
                INSERT INTO curated.dim_product (
                    product_id,
                    product_key,
                    product_name,
                    category,
                    subcategory,
                    maintenance,
                    product_line,
                    product_cost,
                    start_date,
                    end_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            product_id,
            product_key,
            product_name,
            category,
            subcategory,
            maintenance,
            product_line,
            product_cost,
            start_date,
            end_date)

        conn.commit()

        return product_df