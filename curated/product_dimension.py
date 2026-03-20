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
            product_name VARCHAR(255),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            maintenance VARCHAR(50),
            product_line VARCHAR(50),
            product_cost DECIMAL(18,2),
            start_date DATE,
            end_date DATE
        )
        """)
        conn.commit()

        product_info = pd.read_sql("""
            SELECT *
            FROM transformation.prd_info
        """, conn)

        product_category = pd.read_sql("""
            SELECT *
            FROM transformation.px_cat_g1v2
        """, conn)

        product_info.columns = product_info.columns.str.strip()
        product_category.columns = product_category.columns.str.strip()

        product_info["prd_cat"] = (
            product_info["prd_cat"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        product_category["ID"] = (
            product_category["ID"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        product_df = product_info.merge(
            product_category,
            how="left",
            left_on="prd_cat",
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

        product_df["product_cost"] = pd.to_numeric(product_df["product_cost"], errors="coerce")
        product_df["start_date"] = pd.to_datetime(product_df["start_date"], errors="coerce").dt.date
        product_df["end_date"] = pd.to_datetime(product_df["end_date"], errors="coerce").dt.date

        product_df = product_df.where(pd.notna(product_df), None)

        cur.execute("TRUNCATE TABLE curated.dim_product")
        conn.commit()

        rows = [
            (
                int(row["product_id"]) if row["product_id"] is not None else None,
                str(row["product_key"]) if row["product_key"] is not None else None,
                str(row["product_name"]) if row["product_name"] is not None else None,
                str(row["category"]) if row["category"] is not None else None,
                str(row["subcategory"]) if row["subcategory"] is not None else None,
                str(row["maintenance"]) if row["maintenance"] is not None else None,
                str(row["product_line"]) if row["product_line"] is not None else None,
                float(row["product_cost"]) if row["product_cost"] is not None else None,
                row["start_date"],
                row["end_date"]
            )
            for _, row in product_df.iterrows()
        ]

        cur.executemany("""
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
        """, rows)

        conn.commit()

        return product_df