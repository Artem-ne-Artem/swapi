import os


DB_CONN = {
    "host": "postgres_db_dwh",
    "port": 5432,
    "user": os.getenv("POSTGRES_DWH_USER"),
    "password": os.getenv("POSTGRES_DWH_PASSWORD"),
    "DB_NAME": "swapi"
}