import logfire
import os
import requests
import time
from database import Base, BitcoinPrice
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask
from logging import basicConfig, getLogger, INFO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
# from tinydb import TinyDB

# 1. ENVIRONMENT VARIABLES
## 1.1 Load environment variables
load_dotenv()

## 1.2 Read the separated variables from the .env file (non-SSL)
### 1.2.1 APP Variables
APP_HOST = os.getenv("APP_HOST")
APP_PORT = int(os.getenv("APP_PORT"))

### 1.2.2 Database Variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# ------------------------------------------------------
# 1. LOGFIRE SETTINGS
logfire.configure()
basicConfig(handlers=[logfire.LogfireLoggingHandler()])
logger = getLogger(__name__)
logger.setLevel(INFO)
logfire.instrument_requests()
# logfire.instrument_sqlalchemy()

# ------------------------------------------------------
# 2. DATABASE SETTINGS
## 2.1 Construct the PostgreSQL connection URL (non-SSL)
DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

## 2.2 Create the engine and session
engine = create_engine(url=DATABASE_URL)
Session = sessionmaker(bind=engine)
# ------------------------------------------------------

# 3. FUNCTIONS
app = Flask(__name__)

@app.route('/')
def health_check():
    return 'Service is running!'

def create_table():
    """Creates the database table if it does not already exist."""
    Base.metadata.create_all(bind=engine)
    logger.info("Table successfully created or verified!")

def extract_bitcoin_data():
    """Extracts the current Bitcoin price from the Coinbase API."""
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url=url, timeout=15)
    if response.status_code == 200:
        return response.json()
    logger.error(f"API error: {response.status_code}")
    return None


def transform_bitcoin_data(data):
    """Transforms raw data from API, rename columns and add timestamp."""
    value = float(data["data"]["amount"])
    criptocurrency = data["data"]["base"]
    currency = data["data"]["currency"]
    timestamp = datetime.now().isoformat()

    transformed_data = {
        "value": value,
        "criptocurrency": criptocurrency,
        "currency": currency,
        "timestamp": timestamp
    }

    return transformed_data

# def save_data_to_tinydb(data, db_name = "bitcoin.json"):
#     """Load transformed data into TinyDB."""
#     db = TinyDB(db_name)
#     db.insert(data)
#     logger.info("Data successfully saved to TinyDB!")

def save_data_to_postgres(data):
    """Saves the data to the PostgreSQL database."""
    session = Session()
    try:
        new_register = BitcoinPrice(**data)
        session.add(new_register)
        session.commit()
        logger.info(f"[{data['timestamp']}] Data successfully saved to PostgreSQL!")
    except Exception as ex:
        logger.error(f"Error while inserting data into PostgreSQL: {ex}")
        session.rollback()
    finally:
        session.close()

def bitcoin_pipeline():
    """Executes the Bitcoin ETL pipeline with Logfire spans."""
    with logfire.span("Executing Bitcoin ETL Pipeline"):
        with logfire.span("Extract Data from Coinbase API"):
            json_data = extract_bitcoin_data()

        if not json_data:
            logger.error("Data extraction failed. Aborting pipeline.")
            return

        with logfire.span("Transform Bitcoin Data"):
            transformed_data = transform_bitcoin_data(data=json_data)

        # with logfire.span("Save Data to TinyDB"):
        #     save_data_to_tinydb(data=transformed_data)

        with logfire.span("Save Data to PostgreSQL"):
            save_data_to_postgres(data=transformed_data)

        # Example of a final log with placeholders
        logger.info("Pipeline successfully completed!")

def start_server():
    app.run(host=APP_HOST, port=APP_PORT, debug=True)

if __name__ == "__main__":
    create_table()
    logger.info("Starting ETL pipeline with updates every 15 seconds... (Press CTRL+C to stop)")

    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.start()

    while True:
        try:
            bitcoin_pipeline()
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("Process interrupted by the user. Terminating...")
            break
        except Exception as e:
            logger.error(f"Unexpected error during the pipeline: {e}")
            time.sleep(15)
        finally:
            logger.info("Ending pipeline execution!")