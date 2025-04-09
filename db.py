import streamlit as st
import psycopg2
import pandas as pd  # ✅ this was missing!

DB_HOST = "scout-database.ca51kangyonq.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "postgres"


def get_connection():
    creds = st.session_state.get("db_credentials")
    if creds and st.session_state.get("authenticated"):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=creds["username"],
                password=creds["password"],
                host=DB_HOST,
                port=DB_PORT
            )
            return conn
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return None
    else:
        raise ValueError("Missing login credentials in session. Please log in again.")


def run_query(query):
    conn = get_connection()
    if conn is None:
        st.error("Failed to connect to the database.")
        return pd.DataFrame()

    df = pd.read_sql_query(query, conn)

    # Convert sales columns to numeric if present
    if "Sales # (12 Mo.)" in df.columns:
        df["Sales # (12 Mo.)"] = pd.to_numeric(
            df["Sales # (12 Mo.)"].str.replace(",", "", regex=False),
            errors="coerce"
        )
    if "Sales $ (12 Mo.)" in df.columns:
        df["Sales $ (12 Mo.)"] = pd.to_numeric(
            df["Sales $ (12 Mo.)"].str.replace("[$,]", "", regex=True),
            errors="coerce"
        )

    conn.close()
    return df


def build_query(table, filters=None, limit=None):
    """
    Dynamically build a SQL query.

    Parameters:
        table (str): The name of the table to query.
        filters (dict, optional): A dictionary of column names to filter values. For example: {'status': 'active'}.
        limit (int, optional): Limit for the number of rows returned.

    Returns:
        str: The constructed SQL query.
    """
    query = f"SELECT * FROM {table}"

    if filters:
        conditions = []
        for col, val in filters.items():
            # Quote the value if it's a string
            if isinstance(val, str):
                conditions.append(f"{col} = '{val}'")
            else:
                conditions.append(f"{col} = {val}")
        query += " WHERE " + " AND ".join(conditions)

    if limit is not None:
        query += f" LIMIT {limit}"

    return query