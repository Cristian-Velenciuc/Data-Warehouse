from ingestion import create_tables_ingestion
from ingestion import load_data_ingestion
from base_codes import create_schema_ingestion

def main():
    print("\nStarting pipeline")
    ###

    create_schema_ingestion()
    print("\nIngestion Schema Created")

    create_tables_ingestion()
    print("\nIngestion: Tables Created")

    load_data_ingestion()
    print("\nIngestion: Data Loaded")

    ###
    print("\nPipeline finished")

if __name__ == "__main__":
    main()