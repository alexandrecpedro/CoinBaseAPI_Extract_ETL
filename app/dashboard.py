import os
import pandas as pd
import psycopg2
import streamlit as st
import time
from datetime import datetime
from dotenv import load_dotenv

# 1. DATABASE SETTINGS
## 1.1 Load environment variables
load_dotenv()

## 1.2 Read the separated variables from the .env file (non-SSL)
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

def read_postgres_data():
    """Reads data from PostgreSQL database and returns a DataFrame."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        query = "SELECT * FROM bitcoin_prices"
        df = pd.read_sql(query, conn)
        conn.close()

    except Exception as e:
        st.error(f"Error while connecting to PostgreSQL: {ex}")
        return pd.DataFrame()