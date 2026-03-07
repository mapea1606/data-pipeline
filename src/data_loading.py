# src/data_loading.py

import pandas as pd
from sqlalchemy import create_engine, MetaData, text, Column, Integer, String
from sqlalchemy.dialects.postgresql import insert # For UPSERT
from config.logging_config import setup_logger
import os
from dotenv import load_dotenv

# Initialize logger
logger = setup_logger(__name__, log_file='logs/loading.log')

# Load environment variables
load_dotenv()

# Import the transformation script
from data_transformation import transform_data

def load_data_to_final_tables(db_params):
    """
    Loads transformed charges and companies DataFrames into final tables
    in the PostgreSQL database usign UPSERT for idempotency.
    """
    try:
        logger.info(f"Connecting to database at {db_params['host']}")
        # Create a SQLAlchemy engine for connection
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['db_name']}"
        )

        # Get the two clean dataframes from the transformation script
        charges_df, companies_df = transform_data(db_params)
        logger.info(f"Retrieved {len(companies_df)} companies and {len(charges_df)} charges for loading.")

        # --- Create tables with UNIQUE constraints (if not exist) ---
        logger.info("Ensuring tables exist with proper constraints...")
        create_companies_table(engine)
        create_charges_table(engine)

        # --- Indempotent Load ---

        # Load companies_df first (parent table for FK)
        upsert_dataframe(companies_df, 'companies', engine, conflict_column='id')


        # Load charges_df (child table)
        upsert_dataframe(charges_df, 'charges', engine, conflict_column='id')

        # --- Create/Refresh view ---
        create_daily_charges_view(engine)

        logger.info("Idempotent data loading completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during final data loading: {e}", exc_info=True)
        raise

def create_companies_table(engine):
    """Creates companies table with UNIQUE constraint on company_id."""
    query = """
        CREATE TABLE IF NOT EXISTS companies (
            id TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    with engine.begin() as conn:
        conn.execute(text(query))
    logger.info("Table 'companies' ensured with PRIMARY KEY on 'company_id'")

def create_charges_table(engine):
    """Creates charges table with UNIQUE constraint on id and FK to companies."""
    query = """
        CREATE TABLE IF NOT EXISTS charges (
            id TEXT PRIMARY KEY,
            company_id TEXT REFERENCES companies(company_id),
            amount NUMERIC,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    with engine.begin() as conn:
        conn.execute(text(query))
    logger.info("Table 'charges' ensured with PRIMARY KEY on 'id'")

def upsert_dataframe(df: pd.DataFrame, table_name: str, engine, conflict_column: str):
    """
    Performs UPSERT (INSERT ... ON CONFLICT DO UPDATE) for idempotent loading.
    Uses raw SQL for simplicity and reliability.
    """
    if df.empty:
        logger.warning(f"No records to upsert into '{table_name}'")
        return 0
    
    # Make a copy to avoid modifying the original DataFrame
    df = df.copy()
    
    # Handle datetime columns: replace pd.NaT with None for PostgreSQL
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    
    # Handle other null types (NaN, None)
    df = df.where(pd.notnull(df), None)
    
    # Get column names
    columns = list(df.columns)
    columns_str = ', '.join(columns)
    placeholders = ', '.join([f':{col}' for col in columns])
    
    # Build UPDATE clause (exclude conflict_column from updates)
    update_columns = [f'{col} = EXCLUDED.{col}' for col in columns if col != conflict_column]
    update_clause = ', '.join(update_columns) if update_columns else f'{conflict_column} = EXCLUDED.{conflict_column}'
    
    # Build raw SQL UPSERT query
    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_column}) DO UPDATE SET
        {update_clause}
    """
    
    # Execute the upsert
    records = df.to_dict('records')
    with engine.begin() as conn:
        conn.execute(text(query), records)
    
    logger.info(f"Upserted {len(df)} records into '{table_name}'")
    return len(df)

def create_daily_charges_view(engine):
    """Creates or replaces a view for analytics."""
    query = """
        CREATE OR REPLACE VIEW daily_company_charges AS
        SELECT
            c.company_name,
            DATE(ch.created_at) AS charge_date,
            COUNT(ch.id) AS charge_count,
            SUM(ch.amount) AS total_amount
        FROM charges ch
        JOIN companies c ON ch.company_id = c.company_id
        GROUP BY c.company_name, DATE(ch.created_at)
    """
    with engine.begin() as conn:
        conn.execute(text(query))
    logger.info("View 'daily_company_charges' created/refreshed.")

if __name__ == '__main__':
    # Load credentials from environment variables 
    db_params = {
        'db_name': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

    # Validate required variables
    if not all([db_params['db_name'], db_params['user'], db_params['password']]):
        raise ValueError("Missing required environment variables for database connection")

    logger.info("--- Starting Final Data Loading ---")
    load_data_to_final_tables(db_params)
    logger.info("--- Final Data Loading Completed ---")
