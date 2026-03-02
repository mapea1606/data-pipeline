import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from config.logging_config import setup_logger

# Initializer logger
logger = setup_logger(__name__, log_file='logs/ingestion.log')

# Load environment variables from .env file
load_dotenv()

def load_data_to_postgres(csv_file_path, db_params):
    """
    Loads data from a CSV file into a PostgreSQL database staging table.
    
    Args:
        csv_file_path (str): The path to the input CSV file.
        db_params (dict): A dictionary containing database connection parameters.
    """
    try:
        logger.info(f"Connecting to database at {db_params['host']}")
        # Create a SQLAlchemy engine for connection
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['db_name']}"
        )

        logger.info(f"Reading CSV file: {csv_file_path}")
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)
        logger.info(f"Loaded {len(df)} rows from '{csv_file_path}'.")

        # Handle null values in key columns before loading
        # The 'id' column is crucial, so we'll drop rows where it's missing.
        initial_rows = len(df)
        df.dropna(subset=['id'], inplace=True)
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            logger.warning(f"Dropped {dropped_rows} rows with null 'id'.")

        logger.info("Loading data into 'raw_charges' table...") 
        # Load the DataFrame into the 'raw_charges' staging table
        df.to_sql('raw_charges', engine, if_exists='replace', index=False)
        logger.info("Data loaded successfully.")

    except FileNotFoundError:
        logger.error(f"Error: The file '{csv_file_path}' was not found.")
        raise
    except Exception as e:
        logger.error(f"An error occurred during data ingestion: {e}")
        raise

if __name__ == '__main__':
    # Define file path
    csv_file_path = 'data/raw/compras.csv' 

    # Load database credentials from environment variables
    db_params = {
        'db_name': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'), 
        'port': os.getenv('DB_PORT', '5432')
    }

    # Validate that required variables are present 
    if not all([db_params['db_name'], db_params['user'], db_params['password']]):
        raise ValueError("Missing required environment variables: DB_NAME, DB_USER or DB_PASSWORD")

    logger.info("Starting Data Ingestion Pipeline.")
    load_data_to_postgres(csv_file_path, db_params)
    logger.info("Data Ingestion Completed.")
