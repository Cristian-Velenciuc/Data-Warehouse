from base_codes import connect


# Create schemas if they dont exist
def create_schema_ingestion():
    with connect("test_database") as conn:
        cur = conn.cursor()

        cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM sys.schemas WHERE name = 'ingestion'
        )
        EXEC('CREATE SCHEMA ingestion');
        """)

        conn.commit()

if __name__ == "__main__":
    create_schema_ingestion()
