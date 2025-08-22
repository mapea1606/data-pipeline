# src/data_loading.py

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, inspect, text
import os

# Import the transformation script
from data_transformation import transform_data

def load_data_to_final_tables(db_params):
    """
    Loads transformed charges and companies DataFrames into final tables
    in the PostgreSQL database.
    """
    try:
        # Create a SQLAlchemy engine for connection
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['db_name']}"
        )
        print("Successfully created SQLAlchemy engine for final loading.")
        
        # We need to make sure the connection is working before we continue
        insp = inspect(engine)
        if not insp.engine.connect():
            print("Error: Could not connect to the database.")
            return

        # Get the two clean dataframes from the transformation script
        charges_df, companies_df = transform_data(db_params)
        
        # --- Drop Existing Views and Tables (in correct order to handle dependencies) ---
        print("Dropping existing final tables and views...")
        with engine.begin() as connection:
            connection.execute(text("DROP VIEW IF EXISTS daily_company_charges;"))
            connection.execute(text("DROP TABLE IF EXISTS charges;"))
            connection.execute(text("DROP TABLE IF EXISTS companies;"))
        print("Existing objects dropped successfully.")
        
        # --- Load DataFrames into Final Tables ---
        
        # Load companies_df first to satisfy the foreign key constraint
        companies_df.to_sql('companies', engine, if_exists='replace', index=False)
        print(f"Loaded {len(companies_df)} rows into 'companies' table.")
        
        # Load charges_df
        charges_df.to_sql('charges', engine, if_exists='replace', index=False)
        print(f"Loaded {len(charges_df)} rows into 'charges' table.")
        
        print("Data loaded successfully into final tables.")

    except Exception as e:
        print(f"An error occurred during final data loading: {e}")
        raise

if __name__ == '__main__':
    # Define database connection parameters
    db_params = {
        'db_name': 'mydatabase',
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'port': '5432'
    }

    print("--- Starting Final Data Loading ---")
    load_data_to_final_tables(db_params)
    print("--- Final Data Loading Completed ---")
