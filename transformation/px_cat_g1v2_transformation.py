from base_codes import connect
import pandas as pd


def transformation_px_cat_g1v2():

    with connect("test_database") as conn:

        df = pd.read_csv("data_source/source_erp/PX_CAT_G1V2.csv")

        df.columns = df.columns.str.strip()

        cur = conn.cursor()

        cur.execute("""
        IF OBJECT_ID('transformation.px_cat_g1v2', 'U') IS NULL
        BEGIN
            CREATE TABLE transformation.px_cat_g1v2 (
                ID VARCHAR(20),
                CAT VARCHAR(50),
                SUBCAT VARCHAR(100),
                MAINTENANCE VARCHAR(10)
            )
        END
        """)

        cur.execute("""
        TRUNCATE TABLE transformation.px_cat_g1v2
        """)
        df = df.astype(object).where(pd.notnull(df), None)

        rows = df.values.tolist()

        insert_query = """
        INSERT INTO transformation.px_cat_g1v2
        (ID, CAT, SUBCAT, MAINTENANCE)
        VALUES (?, ?, ?, ?)
        """

        cur.fast_executemany = True
        cur.executemany(insert_query, rows)

        conn.commit()

if __name__ == "__main__":
    transformation_px_cat_g1v2()