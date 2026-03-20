import pandas as pd
from base_codes import connect


def fact_sales():
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF OBJECT_ID('curated.fact_sales', 'U') IS NULL
        CREATE TABLE curated.fact_sales (
            sales_sk INT IDENTITY(1,1) PRIMARY KEY,
            product_sk INT,
            customer_sk INT,
            order_number VARCHAR(50),
            order_date DATE,
            shipping_date DATE,
            due_date DATE,
            sales DECIMAL(18,2),
            quantity INT,
            price DECIMAL(18,2)
        )
        """)
        conn.commit()

        sales_details = pd.read_sql("""
            SELECT *
            FROM transformation.sales_details
        """, conn)

        dim_product = pd.read_sql("""
            SELECT product_sk, product_id, product_key
            FROM curated.dim_product
        """, conn)

        dim_customer = pd.read_sql("""
            SELECT customer_sk, customer_id, customer_key
            FROM curated.dim_customer
        """, conn)

        sales_details.columns = sales_details.columns.str.strip()
        dim_product.columns = dim_product.columns.str.strip()
        dim_customer.columns = dim_customer.columns.str.strip()

        sales_details["sls_prd_key"] = (
            sales_details["sls_prd_key"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        dim_product["product_key"] = (
            dim_product["product_key"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        sales_details["sls_cust_id"] = pd.to_numeric(
            sales_details["sls_cust_id"],
            errors="coerce"
        )

        dim_customer["customer_id"] = pd.to_numeric(
            dim_customer["customer_id"],
            errors="coerce"
        )

        df = sales_details.merge(
            dim_product,
            how="left",
            left_on="sls_prd_key",
            right_on="product_key"
        )

        df = df.merge(
            dim_customer,
            how="left",
            left_on="sls_cust_id",
            right_on="customer_id"
        )

        fact_df = df[[
            "product_sk",
            "customer_sk",
            "sls_ord_num",
            "sls_order_dt",
            "sls_ship_dt",
            "sls_due_dt",
            "sls_sales",
            "sls_quantity",
            "sls_price"
        ]].copy()

        fact_df = fact_df.rename(columns={
            "sls_ord_num": "order_number",
            "sls_order_dt": "order_date",
            "sls_ship_dt": "shipping_date",
            "sls_due_dt": "due_date",
            "sls_sales": "sales",
            "sls_quantity": "quantity",
            "sls_price": "price"
        })

        fact_df["order_date"] = pd.to_datetime(fact_df["order_date"], errors="coerce").dt.date
        fact_df["shipping_date"] = pd.to_datetime(fact_df["shipping_date"], errors="coerce").dt.date
        fact_df["due_date"] = pd.to_datetime(fact_df["due_date"], errors="coerce").dt.date

        fact_df["sales"] = pd.to_numeric(fact_df["sales"], errors="coerce")
        fact_df["quantity"] = pd.to_numeric(fact_df["quantity"], errors="coerce")
        fact_df["price"] = pd.to_numeric(fact_df["price"], errors="coerce")

        fact_df = fact_df.where(pd.notna(fact_df), None)

        cur.execute("TRUNCATE TABLE curated.fact_sales")
        conn.commit()

        rows = [
            (
                int(row["product_sk"]) if row["product_sk"] is not None else None,
                int(row["customer_sk"]) if row["customer_sk"] is not None else None,
                str(row["order_number"]) if row["order_number"] is not None else None,
                row["order_date"],
                row["shipping_date"],
                row["due_date"],
                float(row["sales"]) if row["sales"] is not None else None,
                int(row["quantity"]) if row["quantity"] is not None else None,
                float(row["price"]) if row["price"] is not None else None
            )
            for _, row in fact_df.iterrows()
        ]

        cur.executemany("""
            INSERT INTO curated.fact_sales (
                product_sk,
                customer_sk,
                order_number,
                order_date,
                shipping_date,
                due_date,
                sales,
                quantity,
                price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()

        return fact_df