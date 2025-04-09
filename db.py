import streamlit as st
import psycopg2
import pandas as pd
import warnings # To suppress potential UserWarning from pandas read_sql_query

# Database connection details (consider moving sensitive parts like host/port to secrets)
DB_HOST = "scout-database.ca51kangyonq.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "postgres"


def get_connection():
    """Establishes a connection to the PostgreSQL database using credentials from session state."""
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
            print("Database connection established successfully.") # Debug print
            return conn
        except psycopg2.Error as e: # Catch specific psycopg2 errors
            st.error(f"Database connection error: {e}")
            print(f"Database connection error: {e}") # Also print to console
            return None
        except Exception as e:
             st.error(f"An unexpected error occurred during connection: {e}")
             print(f"An unexpected error occurred during connection: {e}")
             return None
    else:
        # It's usually better to return None and let caller handle, than raise error here
        st.warning("Missing database credentials or not authenticated in session state.")
        print("Missing database credentials or not authenticated in session state.")
        return None


# --- run_query (Corrected) ---
def run_query(query, params=None):
    """
    Executes a SQL query against the database with optional parameters.

    Args:
        query (str): The SQL query string (can contain placeholders like %(key)s).
        params (dict, optional): A dictionary of parameters to bind to the query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the query results, or an empty DataFrame on error.
    """
    print(f"run_query called. Query: {query[:200]}... Params: {params}") # Log query start and params
    conn = None # Initialize conn to None
    try:
        conn = get_connection()
        if conn is None:
            st.error("Failed to get database connection.")
            return pd.DataFrame() # Return empty DataFrame if connection failed

        # Use pandas read_sql_query, passing parameters correctly
        # Suppress UserWarning about SQLAlchemy connectable which is not relevant here
        with warnings.catch_warnings():
             warnings.simplefilter("ignore", UserWarning)
             df = pd.read_sql_query(query, conn, params=params)
        print(f"Query executed successfully. Rows returned: {len(df)}")
        return df

    except pd.errors.DatabaseError as e: # Catch pandas DB errors specifically
        st.error(f"Database query execution error: {e}")
        print(f"Database query execution error: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        st.error(f"An unexpected error occurred during query execution: {e}")
        print(f"An unexpected error occurred during query execution: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    finally:
        if conn is not None:
            try:
                conn.close()
                print("Database connection closed.")
            except Exception as e:
                 print(f"Error closing database connection: {e}")


# --- build_query (Commented Out - Unsafe) ---
# def build_query(table, filters=None, limit=None):
#     """
#     🚨 WARNING: This function is commented out due to severe SQL injection vulnerabilities.
#     Do NOT use manual string formatting to build SQL queries with external input.
#     Use parameterized queries (like the updated run_query) instead.

#     Dynamically build a SQL query.

#     Parameters:
#         table (str): The name of the table to query.
#         filters (dict, optional): A dictionary of column names to filter values. For example: {'status': 'active'}.
#         limit (int, optional): Limit for the number of rows returned.

#     Returns:
#         str: The constructed SQL query.
#     """
#     # query = f"SELECT * FROM {table}" # Unsafe table name formatting too

#     # if filters:
#     #     conditions = []
#     #     for col, val in filters.items():
#     #         # Quote the value if it's a string - THIS IS NOT SAFE ENOUGH
#     #         if isinstance(val, str):
#     #             conditions.append(f"{col} = '{val}'") # SQL INJECTION RISK
#     #         else:
#     #             conditions.append(f"{col} = {val}") # SQL INJECTION RISK
#     #     query += " WHERE " + " AND ".join(conditions)

#     # if limit is not None:
#     #     query += f" LIMIT {limit}" # SQL INJECTION RISK

#     # return query
#     print("WARNING: build_query function is unsafe and disabled.")
#     return "-- build_query is disabled due to security risks --"

