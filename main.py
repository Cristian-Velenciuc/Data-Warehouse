from base_codes import create_schemas
from ingestion import create_tables_ingestion
from ingestion import load_data_ingestion
from transformation import create_tables_transformation
from transformation import run_transformation
from curated import customer_dimension
from curated import product_dimension
from curated import fact_sales
import warnings

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable"
)

def main():
    print("\nStarting pipeline")
    ###

    create_schemas()
    print("\nAll Schemas were Created")

    create_tables_ingestion()
    print("\nIngestion: Tables Created")

    load_data_ingestion()
    print("\nIngestion: Data Loaded")

    print("\nTrasformation Phase Start:")
    create_tables_transformation()
    print("\nTransformation: Tables Created")

    run_transformation()
    print("\nTransformation Complete, data was cleaned and saved")

    print("\nCurated Phase Start:")
    print("\nCreating Customer Dimension")
    customer_dimension()

    print("\nCreating Product Dimension")
    product_dimension()

    print("\nCreating the Fact Table")
    fact_sales()

    ###
    print("\nPipeline finished")

if __name__ == "__main__":
    main()