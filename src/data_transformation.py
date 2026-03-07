import os
from dotenv import load_dotenv
from config.logging_config import setup_logger
import pandas as pd
from sqlalchemy import create_engine
import re

# Initialize logger
logger = setup_logger(__name__, log_file='logs/transformation.log')
load_dotenv()

def transform_data(db_params):
    """
    Extracts and transforms data for the charges and companies tables.
    """
    try:
        # Create a SQLAlchemy engine for connection
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['db_name']}"
        )
        logger.info("Successfully created SQLAlchemy engine for transformation.")

        # --- 1.2 Extracción (Extraction) ---
        query = "SELECT * FROM raw_charges"
        df = pd.read_sql(query, engine)
        logger.info(f"Extracted {len(df)} rows from 'raw_charges' table.")

        # --- 1.3 Transformación (Transformation) ---
        
        # 1. Rename columns to match the target schema
        df.rename(columns={
            'name': 'company_name', 
            'paid_at': 'updated_at'
        }, inplace=True)

        # 2. Convert string dates to datetime objects
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        logger.info("Date columns converted to datetime objects.")

        # Drop rows with invalid created_at dates
        initial_rows_with_dates = len(df)
        df.dropna(subset=['created_at'], inplace=True)
        logger.info(f"Dropped {initial_rows_with_dates - len(df)} rows with invalid 'created_at' dates.")

        # 3. Create the 'companies' DataFrame 
        
        # First, drop rows with null company_name
        initial_rows = len(df)
        df.dropna(subset=['company_name'], inplace=True)
        logger.info(f"Dropped {initial_rows - len(df)} rows with null 'company_name'.")
        
        # Load configurable company name mapping
        company_mapping = load_company_mapping()

        # Apply the configurable cleaning function
        df['canonical_company_name'] = df['company_name'].apply(lambda x: get_canonical_name(x, company_mapping))       

        # Filter out rows with bad IDs and those that couldn't be mapped
        cleaned_companies_df = df[~df['company_id'].isin([None, '*******'])]
        cleaned_companies_df = cleaned_companies_df.dropna(subset=['canonical_company_name'])
        
        # Create the companies_df by using the canonical name as the unique identifier
        companies_df = cleaned_companies_df.drop_duplicates(subset='canonical_company_name', keep='first')
        
        # Select and rename the final columns
        companies_df = companies_df[['company_id', 'canonical_company_name']].reset_index(drop=True)
        companies_df.rename(columns={'company_id': 'id', 'canonical_company_name': 'company_name'}, inplace=True)
        
        logger.info(f"Created 'companies' DataFrame with {len(companies_df)} unique companies.")
        
        # 4. Create the 'charges' DataFrame
        # Select and reorder columns
        charges_df = df[[
            'id', 
            'company_id', 
            'amount', 
            'status', 
            'created_at', 
            'updated_at'
        ]].copy()
        
        # Ensure 'id' is a varchar as specified in the schema
        charges_df['id'] = charges_df['id'].astype(str)
        
        # The 'amount' column is already a float64, which is suitable for decimal(16,2)
        logger.info("Created 'charges' DataFrame with required columns.")

        # Validate schemas and referential integrity
        validate_charges_schema(charges_df)
        validate_companies_schema(companies_df)

        # Check referential integrity: all charges must have a valid company_id
        orphaned = ~charges_df['company_id'].isin(companies_df['id'])
        if orphaned.any():
            logger.warning(f"Found {orphaned.sum()} charges with invalid company_id. Dropping them.")
            charges_df = charges_df[~orphaned].copy()

        logger.info("Data validation and referential integrity checks passed")

        return charges_df, companies_df

    except Exception as e:
        logger.error(f"An error occurred during data transformation: {e}", exc_info=True)
        raise

def validate_charges_schema(df: pd.DataFrame) -> bool:
    """Validates charges DataFrame meets expected schema and constraints."""
    required_cols = ['id', 'company_id', 'amount', 'status', 'created_at']
    assert all(col in df.columns for col in required_cols), f"Missing columns. Expected: {required_cols}"
    assert df['id'].notnull().all(), "Null values found in 'id' column"
    assert (df['amount'] >= 0).all(), "Negative amounts detected in 'amount' column"
    logger.info("Charges schema validation passed")
    return True

def validate_companies_schema(df: pd.DataFrame) -> bool:
    """Validates companies DataFrame meets expected schema."""
    assert 'id' in df.columns and 'company_name' in df.columns, "Missing required columns in companies"
    assert df['id'].notnull().all(), "Null company IDs found"
    assert df['company_name'].notnull().all(), "Null company names found"
    logger.info("Companies schema validation passed")
    return True

def load_company_mapping(mapping_path: str = 'config/company_mapping.csv') -> dict:
    """Loads company name mapping from CSV file."""
    import csv
    from pathlib import Path

    mapping = {}
    path = Path(mapping_path)
    if path.exists():
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping[row['raw_name_pattern']] = row['canonical_name']
        logger.info(f"Loaded {len(mapping)} company name mappings from {mapping_path}")
    else:
        logger.warning(f"Company mapping file not found at {mapping_path}. Using empty mapping.")
    return mapping

def get_canonical_name(name: str, mapping: dict) -> str | None:
    """Maps raw company names to canonical names using a configurable dictionary."""
    if pd.isna(name):
        return None
    name_lower = str(name).lower().strip()

    # Check if any pattern matches
    for pattern, canonical in mapping.items():
        if pattern.lower() in name_lower:
            return canonical
    return None  # Unmapped names will be dropped later

if __name__ == '__main__':
    # Define database connection parameters
    db_params = {
        'db_name': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

    if not all([db_params['db_name'], db_params['user'], db_params['password']]):
        raise ValueError("Missing required environment variables.")

    logger.info("--- Starting Data Transformation ---")
    charges_data, companies_data = transform_data(db_params)
    logger.info(f"Transformation complete: {len(companies_data)} companies, {len(charges_data)} charges")
    logger.info(f"Sample charges: {charges_data.head(2).to_dict('records')}")
    logger.info("--- Data Transformation Completed ---")
