import streamlit as st
from db import run_query
from filters import apply_text_filter, apply_multiselect_filter

def load_data():
    query = """
    SELECT
      email AS "Email",
      agent_first_name AS "First name",
      agent_last_name AS "Last name",
      office_name AS "Brokerage",
      office_address_1 AS "Address 1",
      office_address_2 AS "Address 2",
      office_city AS "City",
      office_state AS "State",
      office_zip AS "Zip",
      office_phone AS "Phone",
      license_type AS "License type",
      license_number AS "License number",
      association AS "Association"
    FROM agents_master
    LIMIT 1000;
    """
    return run_query(query)

def agents_view():
    st.title("Agents View")
    df = load_data()
    df = apply_text_filter(df, "City", "City")
    df = apply_multiselect_filter(df, "State", "State")
    st.dataframe(df, use_container_width=True)
    st.metric("Total Rows", len(df))
    st.download_button("Export as CSV", data=df.to_csv(index=False).encode('utf-8'), file_name="agents.csv", mime="text/csv")
