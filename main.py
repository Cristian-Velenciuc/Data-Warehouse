from base_codes import create_schemas
from ingestion import create_tables_ingestion
from ingestion import load_data_ingestion
from transformation import create_tables_transformation
from transformation import run_transformation

def main():
    print("\nStarting pipeline")
    ###

    create_schemas()
    print("\nAll Schemas were Created")

    create_tables_ingestion()
    print("\nIngestion: Tables Created")

    load_data_ingestion()
    print("\nIngestion: Data Loaded")

    create_tables_transformation()
    print("\nTransformation: Tables Created")

    print("\nTrasformation Phase Start:")
    run_transformation()
    print("\nTransformation Complete, data was cleaned and saved")


    ###
    print("\nPipeline finished")

if __name__ == "__main__":
    main()