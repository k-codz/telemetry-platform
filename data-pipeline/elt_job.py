import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION
# ==========================================
POSTGRES_URL = "postgresql://admin:secretpassword@host.docker.internal:5432/telemetry"
CLICKHOUSE_HOST = "host.docker.internal"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "telemetry"

def extract_from_postgres() -> pd.DataFrame:
    """Extracts raw event data from the PostgreSQL OLTP database."""
    logger.info("Connecting to PostgreSQL...")
    engine = create_engine(POSTGRES_URL)
    
    # In a production environment, we would use a "watermark" (e.g., last_extracted_at)
    # to only pull new records (Incremental Load). For this project, we'll pull all records.
    query = """
        SELECT user_id, event_type, payload, occurred_at 
        FROM events;
    """
    
    logger.info("Executing extraction query...")
    df = pd.read_sql(query, engine)
    logger.info(f"Successfully extracted {len(df)} rows from PostgreSQL.")
    
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs lightweight transformations to match the ClickHouse schema."""
    if df.empty:
        return df
        
    logger.info("Applying basic data transformations...")
    
    # ClickHouse's DateTime expects timezone-naive pandas datetimes or specific tz handling.
    # We strip the timezone info for simplicity in this project.
    df['occurred_at'] = pd.to_datetime(df['occurred_at']).dt.tz_localize(None)
    
    # Ensure payload is a string (ClickHouse expects a String for this column)
    df['payload'] = df['payload'].astype(str)
    
    return df

def load_to_clickhouse(df: pd.DataFrame):
    """Bulk inserts the Pandas DataFrame into ClickHouse."""
    if df.empty:
        logger.info("No data to load. Exiting.")
        return

    logger.info("Connecting to ClickHouse...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            database=CLICKHOUSE_DB
        )
        
        logger.info("Inserting data into ClickHouse...")
        # clickhouse-connect has a highly optimized method specifically for Pandas DataFrames
        client.insert_df('events_analytical', df)
        logger.info("Successfully loaded data into ClickHouse!")
        
    except Exception as e:
        logger.error(f"Failed to load data into ClickHouse: {e}")
        sys.exit(1)

def main():
    logger.info("Starting ELT Pipeline...")
    
    # 1. EXTRACT
    raw_df = extract_from_postgres()
    
    # 2. TRANSFORM (Lightweight preparation)
    clean_df = transform_data(raw_df)
    
    # 3. LOAD
    load_to_clickhouse(clean_df)
    
    logger.info("ELT Pipeline completed successfully.")

if __name__ == "__main__":
    main()