import streamlit as st
import psycopg2

DB_HOST = "scout-database.ca51kangyonq.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "postgres"


def login():
    # Ensure keys exist in session state
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "username" not in st.session_state:
        st.session_state.username = ""
    if "password" not in st.session_state:
        st.session_state.password = ""

    if not st.session_state.authenticated:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
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
                # Store credentials in a dedicated key for later use if needed
                st.session_state.db_credentials = {
                    "username": st.session_state.username,
                    "password": st.session_state.password
                }
                st.success("Logged in successfully")
                st.rerun()
            except Exception as e:
                st.error("Login failed: " + str(e))


def logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.success("Logged out successfully")
    st.rerun()


def is_authenticated():
    return st.session_state.get("authenticated", False)