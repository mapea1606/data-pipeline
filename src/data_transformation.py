import pandas as pd
from sqlalchemy import create_engine
import re

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
        print("Successfully created SQLAlchemy engine for transformation.")

        # --- 1.2 Extracción (Extraction) ---
        query = "SELECT * FROM raw_charges"
        df = pd.read_sql(query, engine)
        print(f"Extracted {len(df)} rows from 'raw_charges' table.")

        # --- 1.3 Transformación (Transformation) ---
        
        # 1. Rename columns to match the target schema
        df.rename(columns={
            'name': 'company_name', 
            'paid_at': 'updated_at'
        }, inplace=True)

        # 2. Convert string dates to datetime objects
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        print("Date columns converted to datetime objects.")

        # Drop rows with invalid created_at dates
        initial_rows_with_dates = len(df)
        df.dropna(subset=['created_at'], inplace=True)
        print(f"Dropped {initial_rows_with_dates - len(df)} rows with invalid 'created_at' dates.")

        # 3. Create the 'companies' DataFrame 
        
        # First, drop rows with null company_name
        initial_rows = len(df)
        df.dropna(subset=['company_name'], inplace=True)
        print(f"Dropped {initial_rows - len(df)} rows with null 'company_name'.")
        
        # Define a cleaning function to handle inconsistencies and provide a canonical name
        def get_canonical_name(name):
            if pd.isna(name):
                return None
            name = str(name).lower().strip()
            
            # Mapping messy data to a clean, canonical name
            if "mipasajefy" in name:
                return "MiPasajefy"
            if "muebles chidos" in name:
                return "Muebles chidos"
            return None # Drop any other inconsistent names that are not our two companies
        
        # Apply the cleaning function to the company_name column
        df['canonical_company_name'] = df['company_name'].apply(get_canonical_name)
        
        # Filter out rows with bad IDs and those that couldn't be mapped
        cleaned_companies_df = df[~df['company_id'].isin([None, '*******'])]
        cleaned_companies_df = cleaned_companies_df.dropna(subset=['canonical_company_name'])
        
        # Create the companies_df by using the canonical name as the unique identifier
        companies_df = cleaned_companies_df.drop_duplicates(subset='canonical_company_name', keep='first')
        
        # Select and rename the final columns
        companies_df = companies_df[['company_id', 'canonical_company_name']].reset_index(drop=True)
        companies_df.rename(columns={'company_id': 'id', 'canonical_company_name': 'company_name'}, inplace=True)
        
        print(f"Created 'companies' DataFrame with {len(companies_df)} unique companies.")
        
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
        print("Created 'charges' DataFrame with required columns.")

        return charges_df, companies_df

    except Exception as e:
        print(f"An error occurred during data transformation: {e}")
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

    print("--- Starting Data Transformation ---")
    charges_data, companies_data = transform_data(db_params)
    
    # You can inspect the head of the transformed dataframes
    print("\nCharges DataFrame Head:")
    print(charges_data.head())
    
    print("\nCompanies DataFrame Head:")
    print(companies_data.head())
    
    print(f"\nFinal count for charges_df: {len(charges_data)} rows")
    print(f"Final count for companies_df: {len(companies_data)} rows")

    print("\n--- Data Transformation Completed ---")
