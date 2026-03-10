from base_codes import connect
import os
import pandas as pd

contents_folder = "./data_source"

def load_data_ingestion():

    with connect("test_database") as conn:

        cur = conn.cursor()
        cur.fast_executemany = True

        for root, dirs, files in os.walk(contents_folder):

            for file in files:

                if file.endswith(".csv"):

                    print(f"Loading file: {file}")

                    full_path = os.path.join(root, file)

                    table_name = file.replace(".csv", "")

                    # Clear ingestion table first
                    truncate_query = f"TRUNCATE TABLE ingestion.{table_name}"
                    cur.execute(truncate_query)

                    table_variables = pd.read_csv(full_path)

                    table_variables = table_variables.astype(object).where(
                        pd.notnull(table_variables), None
                    )

                    rows = table_variables.values.tolist()

                    cols = ", ".join(table_variables.columns)

                    placeholders = ", ".join(["?"] * len(table_variables.columns))

                    query = f"""
                    INSERT INTO ingestion.{table_name} ({cols})
                    VALUES ({placeholders})
                    """

                    cur.executemany(query, rows)

                    conn.commit()

                    print(f"{len(rows)} rows inserted into ingestion.{table_name}")


if __name__ == "__main__":
    load_data_ingestion()