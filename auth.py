import streamlit as st
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file (for local testing)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def login():
    # Initialize session state keys if they don't exist
    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("username", "")
    st.session_state.setdefault("password", "")
    st.session_state.setdefault("db_credentials", {})

    if not st.session_state.authenticated:
        st.session_state.username = st.text_input("Username")
        st.session_state.password = st.text_input("Password", type="password")
        if st.button("Login"):
            try:
                conn = psycopg2.connect(
                    dbname=DB_NAME,
                    user=st.session_state.username,
                    password=st.session_state.password,
                    host=DB_HOST,
                    port=DB_PORT
                )
                conn.close()
                st.session_state.authenticated = True
                st.session_state.db_credentials = {
                    "username": st.session_state.username,
                    "password": st.session_state.password
                }
                st.success("Logged in successfully")
                st.rerun()
            except Exception as e:
                st.error(f"Login failed: {e}")


def logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.success("Logged out successfully")
    st.rerun()


def is_authenticated():
    return st.session_state.get("authenticated", False)

# Optional: Function to get database credentials if needed elsewhere
def get_db_credentials():
    return st.session_state.get("db_credentials", {})