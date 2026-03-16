from base_codes import connect
import pandas as pd


def transformation_sales_details():

    with connect("test_database") as conn:

        df = pd.read_sql("SELECT * FROM ingestion.sales_details", conn)

        # Convert dates
        df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"], format="%Y%m%d", errors="coerce")
        df["sls_ship_dt"]  = pd.to_datetime(df["sls_ship_dt"], format="%Y%m%d", errors="coerce")
        df["sls_due_dt"]   = pd.to_datetime(df["sls_due_dt"], format="%Y%m%d", errors="coerce")

        # Same order date for all rows in same order
        order_counts = df.groupby("sls_ord_num")["sls_ord_num"].transform("count")
        group_order_date = df.groupby("sls_ord_num")["sls_order_dt"].transform("min")

        df.loc[order_counts > 1, "sls_order_dt"] = group_order_date[order_counts > 1]

        # Single-item orders → use ship date if order date missing
        df["sls_order_dt"] = df["sls_order_dt"].fillna(df["sls_ship_dt"])

        # Price cleaning
        df["sls_price"] = df["sls_price"].abs()
        df["sls_price"] = df["sls_price"].fillna(
            df["sls_sales"] / df["sls_quantity"].replace(0, pd.NA)
        )

        # Recalculate sales
        df["sls_sales"] = df["sls_quantity"] * df["sls_price"]

        # Convert numbers
        df["sls_sales"] = df["sls_sales"].round(2)
        df["sls_price"] = df["sls_price"].round(2)

        # Convert for SQL
        df["sls_cust_id"] = df["sls_cust_id"].apply(lambda x: None if pd.isna(x) else int(x))
        df["sls_quantity"] = df["sls_quantity"].apply(lambda x: None if pd.isna(x) else int(x))

        # Convert datetime to python date
        df["sls_order_dt"] = df["sls_order_dt"].dt.date
        df["sls_ship_dt"] = df["sls_ship_dt"].dt.date
        df["sls_due_dt"] = df["sls_due_dt"].dt.date

        data = list(df.itertuples(index=False, name=None))

        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE transformation.sales_details")

        insert_sql = """
        INSERT INTO transformation.sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.fast_executemany = True
        cursor.executemany(insert_sql, data)
        conn.commit()

        cursor.close()

        print("Rows inserted:", len(df))


if __name__ == "__main__":
    transformation_sales_details()