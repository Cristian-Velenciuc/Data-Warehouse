from base_codes import connect

def create_tables_transformation():
    # Create tables if they dont exist
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF OBJECT_ID('transformation.cust_info', 'U') IS NULL
        CREATE TABLE transformation.cust_info (
            cst_id INT,
            cst_key VARCHAR(50),
            cst_firstname VARCHAR(100),
            cst_lastname VARCHAR(100),
            cst_marital_status VARCHAR(20),
            cst_gndr VARCHAR(10),
            cst_create_date DATE
        )
        """)

        cur.execute("""
        IF OBJECT_ID('transformation.prd_info', 'U') IS NULL
        CREATE TABLE transformation.prd_info (
            prd_id INT,
            prd_key VARCHAR(50),
            prd_cat VARCHAR(10),
            prd_nm VARCHAR(150),
            prd_cost DECIMAL(10,2),
            prd_line VARCHAR(50),
            prd_start_dt DATE,
            prd_end_dt DATE
        )
        """)

        cur.execute("""
        IF OBJECT_ID('transformation.sales_details', 'U') IS NULL
        CREATE TABLE transformation.sales_details (
            sls_ord_num VARCHAR(20),
            sls_prd_key VARCHAR(50),
            sls_cust_id INT,
            sls_order_dt INT,
            sls_ship_dt INT,
            sls_due_dt INT,
            sls_sales DECIMAL(12,2),
            sls_quantity INT,
            sls_price DECIMAL(12,2)
        )
        """)

        cur.execute("""
        IF OBJECT_ID('transformation.cust_az12', 'U') IS NULL
        CREATE TABLE transformation.cust_az12 (
            CID VARCHAR(50),
            BDATE DATE,
            GEN VARCHAR(20)
        )
        """)

        cur.execute("""
        IF OBJECT_ID('transformation.loc_a101', 'U') IS NULL
        CREATE TABLE transformation.loc_a101 (
            CID VARCHAR(50),
            CNTRY VARCHAR(100)
        )
        """)

        cur.execute("""
        IF OBJECT_ID('transformation.px_cat_g1v2', 'U') IS NULL
        CREATE TABLE transformation.px_cat_g1v2 (
            ID VARCHAR(50),
            CAT VARCHAR(100),
            SUBCAT VARCHAR(100),
            MAINTENANCE VARCHAR(10)
        )
        """)

        conn.commit()

if __name__ == "__main__":
    create_tables_transformation()