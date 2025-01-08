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
        query = "SELECT * FROM bitcoin_prices ORDER BY timestamp DESC"
        df = pd.read_sql(sql=query, con=conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error while connecting to PostgreSQL: {e}")
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="Bitcoin Price Dashboard", layout="wide")
    st.title("📊 Bitcoin Price Dashboard")
    st.write("This dashboard displays Bitcoin price data periodically collected in a PostgreSQL database.")

    df = read_postgres_data()

    if not df.empty:
        st.subheader("📋 Recent Data")
        st.dataframe(data=df)

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')

        st.subheader("📈 Bitcoin Price Evolution")
        st.line_chart(data=df, x='timestamp', y='value', use_container_width=True)

        st.subheader("🔢 General Statistics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Current Price", f"USD {df['value'].iloc[-1]:,.2f}")
        col2.metric("Maximum Price", f"USD {df['value'].max():,.2f}")
        col3.metric("Minimum Price", f"USD {df['value'].min():,.2f}")
    else:
        st.warning("No data found in the PostgreSQL database.")

if __name__ == "__main__":
    main()