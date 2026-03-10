import pyodbc

SERVER = r"DESKTOP-5LAP5VP\SQLEXPRESS"
DRIVER = "ODBC Driver 17 for SQL Server"

def connect(database="master", autocommit=False):
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;",
        autocommit=autocommit
    )