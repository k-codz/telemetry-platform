import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION (Using Environment Variables)
# ==========================================
# If the Env Var doesn't exist, fallback to the local Docker Desktop network!
POSTGRES_URL = os.getenv(
    "POSTGRES_URL", 
    "postgresql://admin:secretpassword@host.docker.internal:5432/telemetry"
)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "host.docker.internal")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "telemetry")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def extract_from_postgres() -> pd.DataFrame:
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_URL.split('@')[-1]}...")
    engine = create_engine(POSTGRES_URL)
    
    query = "SELECT user_id, event_type, payload, occurred_at FROM events;"
    df = pd.read_sql(query, engine)
    
    logger.info(f"Successfully extracted {len(df)} rows from PostgreSQL.")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df['occurred_at'] = pd.to_datetime(df['occurred_at']).dt.tz_localize(None)
    df['payload'] = df['payload'].astype(str)
    return df

def load_to_clickhouse(df: pd.DataFrame):
    if df.empty:
        logger.info("No data to load. Exiting.")
        return

    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            database=CLICKHOUSE_DB,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        
        # IDEMPOTENCY: Wipe the existing data before loading the fresh batch!
        logger.info("Truncating existing data to ensure idempotency...")
        client.command('TRUNCATE TABLE events_analytical')
        
        logger.info("Inserting fresh batch into ClickHouse...")
        client.insert_df('events_analytical', df)
        logger.info("Successfully loaded data into ClickHouse!")
        
    except Exception as e:
        logger.error(f"Failed to load data into ClickHouse: {e}")
        sys.exit(1)

def main():
    logger.info("Starting Orchestrated ELT Pipeline...")
    raw_df = extract_from_postgres()
    clean_df = transform_data(raw_df)
    load_to_clickhouse(clean_df)
    logger.info("ELT Pipeline completed successfully.")

if __name__ == "__main__":
    main()