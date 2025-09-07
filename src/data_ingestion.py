# src/data_ingestion.py

import pandas as pd
from sqlalchemy import create_engine
import os

def load_data_to_postgres(csv_file_path, db_params):
    """
    Loads data from a CSV file into a PostgreSQL database staging table.
    
    Args:
        csv_file_path (str): The path to the input CSV file.
        db_params (dict): A dictionary containing database connection parameters.
    """
    try:
        # Create a SQLAlchemy engine for connection
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['db_name']}"
        )
        print("Successfully created SQLAlchemy engine.")

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)
        print(f"Loaded {len(df)} rows from '{csv_file_path}'.")

        # Handle null values in key columns before loading
        # The 'id' column is crucial, so we'll drop rows where it's missing.
        initial_rows = len(df)
        df.dropna(subset=['id'], inplace=True)
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            print(f"Dropped {dropped_rows} rows with null 'id'.")

        # Load the DataFrame into the 'raw_charges' staging table
        df.to_sql('raw_charges', engine, if_exists='replace', index=False)
        
        print("Data loaded successfully into the 'raw_charges' table.")

    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
        raise
    except Exception as e:
        print(f"An error occurred during data ingestion: {e}")
        raise

if __name__ == '__main__':
    # Define file path and database connection parameters
    # The `data/` directory is relative to the project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_file_path = os.path.join(base_dir, 'data', 'data_prueba_tecnica_1.csv') 

    db_params = {
        'db_name': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost', 
        'port': '5432'
    }

    print("--- Starting Data Ingestion Pipeline ---")
    load_data_to_postgres(csv_file_path, db_params)
    print("--- Data Ingestion Completed ---")
