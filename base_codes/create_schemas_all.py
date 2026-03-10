from base_codes import connect


# Create schemas if they dont exist
def create_schemas():
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM sys.schemas WHERE name = 'ingestion'
        )
        EXEC('CREATE SCHEMA ingestion');
        """)

        cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM sys.schemas WHERE name = 'transformation'
        )
        EXEC('CREATE SCHEMA transformation');
        """)

        conn.commit()

if __name__ == "__main__":
    create_schemas()

