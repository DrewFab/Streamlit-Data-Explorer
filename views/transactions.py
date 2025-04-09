import streamlit as st
from db import run_query
from filters import apply_text_filter

@st.cache_data(ttl=600)
def load_data():
    query = """
    SELECT
      presented_by_first_name AS "Agent First",
      presented_by_last_name AS "Agent Last",
      listing_agent_id AS "Agent MLS ID",
      list_date AS "List Date",
      brokered_by AS "Brokerage",
      listing_office_id AS "Office ID",
      address_line_1 AS "Address 1",
      address_line_2 AS "Address 2",
      city AS "City",
      state AS "State",
      zip_code AS "Zip"
    FROM transactions_2
    LIMIT 1000;
    """
    return run_query(query)

def transactions_view():
    st.title("Transactions View")
    df = load_data()
    df = apply_text_filter(df, "City", "City")
    df = apply_text_filter(df, "Agent Last", "Agent Last Name")
    st.dataframe(df, use_container_width=True)
    st.metric("Total Rows", len(df))
    st.download_button("Export as CSV", data=df.to_csv(index=False).encode('utf-8'), file_name="transactions.csv", mime="text/csv")
