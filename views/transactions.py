import streamlit as st
from db import run_query
import pandas as pd
from datetime import datetime, timedelta

CACHE_LIMIT_TRANSACTIONS = 5000  # Set your desired page size

@st.cache_data(ttl=600)
def load_transactions_data(limit=CACHE_LIMIT_TRANSACTIONS, offset=0, date_range=None, states=None, statuses=None, price_min=None, price_max=None):
    """
    Loads transaction data from the database with optional filters and pagination.

    Args:
        limit (int): The maximum number of rows to retrieve.
        offset (int): The row offset to start retrieving from (for pagination).
        date_range (tuple, optional): A tuple of (start_date, end_date) for filtering by list_date.
        states (list, optional): A list of state abbreviations to filter by.
        statuses (list, optional): A list of transaction statuses to filter by.
        price_min (float, optional): The minimum price to filter by.
        price_max (float, optional): The maximum price to filter by.

    Returns:
        pandas.DataFrame: The transaction data.
    """
    query = """
    SELECT
      email AS "Email",
      presented_by_first_name AS "Agent First",
      presented_by_last_name AS "Agent Last",
      brokered_by AS "Brokerage",
      list_date AS "List Date",
      status AS "Status",
      price AS "Price",
      address_line_1 AS "Address 1",
      address_line_2 AS "Address 2",
      city AS "City",
      state AS "State",
      CAST(zip_code AS TEXT) AS "Zip",
      square_feet AS "SQFT",
      presented_by_mobile AS "Phone",
      listing_agent_id AS "Agent MLS ID",
      listing_office_id AS "Office ID"
    FROM transactions_2
    WHERE 1=1
    """
    params_list = []
    where_clauses = []

    if date_range:
        where_clauses.append("list_date >= %s AND list_date <= %s")
        params_list.extend(date_range)

    if states:
        where_clauses.append("state IN %s")
        params_list.append(tuple(states))

    if statuses:
        where_clauses.append("status IN %s")
        params_list.append(tuple(statuses))

    if price_min is not None:
        where_clauses.append("price >= %s")
        params_list.append(price_min)

    if price_max is not None:
        where_clauses.append("price <= %s")
        params_list.append(price_max)

    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)

    query += " LIMIT %s OFFSET %s;"
    params_list.append(limit)
    params_list.append(offset)

    print("--- Transactions Data Query ---")
    print(f"SQL: {query}")
    print(f"Params: {params_list}")
    print("-------------------------------")

    return run_query(query, params=tuple(params_list))

def get_total_matching_rows(date_range=None, states=None, statuses=None, price_min=None, price_max=None):
    """
    Gets the total number of rows matching the given filters.  Used for pagination.

    Args:
        date_range (tuple, optional): A tuple of (start_date, end_date) for filtering by list_date.
        states (list, optional): A list of state abbreviations to filter by.
        statuses (list, optional): A list of transaction statuses to filter by.
        price_min (float, optional): The minimum price to filter by.
        price_max (float, optional): The maximum price to filter by.

    Returns:
        int: The total number of matching rows.
    """
    query = """
    SELECT COUNT(*)
    FROM transactions_2
    WHERE 1=1
    """
    params_list = []
    where_clauses = []

    if date_range:
        where_clauses.append("list_date >= %s AND list_date <= %s")
        params_list.extend(date_range)

    if states:
        where_clauses.append("state IN %s")
        params_list.append(tuple(states))

    if statuses:
        where_clauses.append("status IN %s")
        params_list.append(tuple(statuses))

    if price_min is not None:
        where_clauses.append("price >= %s")
        params_list.append(price_min)

    if price_max is not None:
        where_clauses.append("price <= %s")
        params_list.append(price_max)

    if where_clauses:
        query += " AND " + " AND ".join(where_clauses)

    result = run_query(query, params=tuple(params_list))
    return result.iloc[0][0] if not result.empty else 0

def format_price(price):
    """Formats the price as $ with commas, handles non-numeric."""
    if pd.isna(price):
        return ""
    try:
        return f"${float(price):,.0f}"
    except (ValueError, TypeError):
        return str(price)  # Return the original value as string

def transactions_view():
    """
    Displays the transactions view with filtering, pagination, and data display.
    """
    st.title("Transactions View")

    with st.sidebar:
        st.header("Filter Transactions")

        # Date Range Filter
        date_options = ["All", "Last Week", "Last Month", "Last Quarter", "Custom Range"]
        default_date_index = date_options.index("Last Week")
        date_filter_option = st.radio(
            "List Date Range",
            date_options,
            index=default_date_index  # Set the index to default to "Last Week"
        )

        date_range = None
        today = datetime.now().date()

        if date_filter_option == "Last Week":
            start_date = today - timedelta(days=7)
            end_date = today
            date_range = (start_date, end_date)
        elif date_filter_option == "Last Month":
            start_date = today - timedelta(days=30)
            end_date = today
            date_range = (start_date, end_date)
        elif date_filter_option == "Last Quarter":
            start_date = today - timedelta(days=90)
            end_date = today
            date_range = (start_date, end_date)
        elif date_filter_option == "Custom Range":
            min_date_db = pd.to_datetime(run_query("SELECT MIN(list_date) FROM transactions_2;")['min'][0]).date()
            max_date_db = pd.to_datetime(run_query("SELECT MAX(list_date) FROM transactions_2;")['max'][0]).date()
            custom_date_range = st.date_input("Select Custom Range", (min_date_db, max_date_db))
            date_range = custom_date_range

        # State Filter
        us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                     'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                     'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                     'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                     'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

        select_all_states = st.checkbox("Select All States", key="select_all_states", value=True) # Make it default
        if select_all_states:
            selected_states = us_states
        else:
            selected_states = st.multiselect(
                "State", us_states, default=st.session_state.get("selected_states", []), key="state_select"
            )
        st.session_state.selected_states = selected_states #save state

        # Status Filter
        all_statuses = ["Pending", "Active", "Sold"]
        selected_statuses = st.multiselect("Status", all_statuses)

        # Price Input Boxes
        min_price_db = float(run_query("SELECT MIN(price) FROM transactions_2;")['min'][0])
        max_price_db = float(run_query("SELECT MAX(price) FROM transactions_2;")['max'][0])

        col1, col2 = st.columns(2)
        with col1:
            min_price_input = st.number_input("Min Price", min_value=min_price_db, max_value=max_price_db, value=min_price_db, step=1.0)
        with col2:
            max_price_input = st.number_input("Max Price", min_value=min_price_db, max_value=99999999.0, value=99999999.0, step=1.0)

        apply_filters = st.button("Apply Filters")

    if apply_filters:
        st.session_state['transactions_offset'] = 0  # Reset pagination on new filter
        df = load_transactions_data(
            limit=CACHE_LIMIT_TRANSACTIONS,
            offset=st.session_state['transactions_offset'],
            date_range=date_range,
            states=selected_states,
            statuses=selected_statuses,
            price_min=min_price_input,
            price_max=max_price_input
        )
        st.session_state['filtered_transactions_data'] = df
        st.session_state['total_matching_rows'] = get_total_matching_rows(
            date_range=date_range,
            states=selected_states,
            statuses=selected_statuses,
            price_min=min_price_input,
            price_max=max_price_input
        )
    elif 'filtered_transactions_data' in st.session_state:
        df_display = st.session_state['filtered_transactions_data'].iloc[
            st.session_state['transactions_offset']:st.session_state['transactions_offset'] + CACHE_LIMIT_TRANSACTIONS
        ]
    else:
        df = load_transactions_data(limit=CACHE_LIMIT_TRANSACTIONS, offset=0)
        st.session_state['filtered_transactions_data'] = df
        st.session_state['total_matching_rows'] = get_total_matching_rows()
        df_display = df

    if 'filtered_transactions_data' in st.session_state:
        df_display = st.session_state['filtered_transactions_data'].iloc[
            st.session_state['transactions_offset']:st.session_state['transactions_offset'] + CACHE_LIMIT_TRANSACTIONS
        ]
    else:
        df_display = pd.DataFrame()

    if not df_display.empty:
        print("Data type of 'Price' column:", df_display["Price"].dtype)
        print("First 20 values of 'Price' column:")
        print(df_display["Price"].head(20))
        df_display["Price"] = df_display["Price"].apply(format_price)
        st.dataframe(df_display, use_container_width=True)

        # --- Metrics and Download ---
        col_dl = st.columns([1,])
        with col_dl[0]:
            csv_data = df_display.to_csv(index=False).encode('utf-8')
            st.download_button(label="Export Displayed Data as CSV", data=csv_data,
                               file_name="filtered_transactions.csv", mime="text/csv", key='download_csv')

        st.metric("Rows Displayed", len(df_display))
        st.metric("Total Rows Matching Filters", st.session_state.get('total_matching_rows', 0))
        start_row = st.session_state.get('transactions_offset', 0) + 1
        end_row = st.session_state.get('transactions_offset', 0) + len(df_display)
        st.caption(f"Showing rows {start_row}-{end_row} of {st.session_state.get('total_matching_rows', 0)}")

        # --- Pagination ---
        total_rows = st.session_state.get('total_matching_rows', 0)
        if total_rows > CACHE_LIMIT_TRANSACTIONS:
            if st.button("Load More",  key="load_more"):
                st.session_state['transactions_offset'] += CACHE_LIMIT_TRANSACTIONS
                st.rerun()

    else:
        st.info("No transactions match the current filters.")
