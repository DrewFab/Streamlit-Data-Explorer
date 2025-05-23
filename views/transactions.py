import streamlit as st
from db import run_query
import pandas as pd
from datetime import datetime, timedelta
import os
from config import EXPORT_PATH

CACHE_LIMIT_TRANSACTIONS = 5000  # Set your desired page size

us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
             'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND',
             'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
all_statuses = ["Pending", "Active", "Sold"]
today = datetime.now().date()


@st.cache_data(ttl=600)
def load_transactions_data(limit=CACHE_LIMIT_TRANSACTIONS, offset=0, date_range=None, states=None, statuses=None,
                           price_min=None, price_max=None):
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

    return run_query(query, params=tuple(params_list))


def get_total_matching_rows(date_range=None, states=None, statuses=None, price_min=None, price_max=None):
    query = """
    SELECT COUNT(*) FROM transactions_2 WHERE 1=1
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
    if pd.isna(price):
        return ""
    try:
        return f"${float(price):,.0f}"
    except (ValueError, TypeError):
        return str(price)


def transactions_view():
    st.title("Transactions View")

    today = datetime.now().date()

    apply_filters = False  # Moved before the sidebar section

    # LIKE filters
    st.session_state.setdefault("filter_brokerage", "")
    st.session_state.setdefault("filter_agent_first", "")
    st.session_state.setdefault("filter_agent_last", "")

    # Defaults
    default_range = (today - timedelta(days=7), today)
    default_states = us_states
    default_statuses = ["Active"]

    st.session_state.setdefault('transactions_offset', 0)
    st.session_state.setdefault('filtered_transactions_data', pd.DataFrame())
    st.session_state.setdefault('total_matching_rows', 0)
    st.session_state.setdefault('date_range', default_range)
    st.session_state.setdefault('selected_states', default_states)
    st.session_state.setdefault('selected_statuses', default_statuses)
    st.session_state.setdefault('min_price', 0)
    st.session_state.setdefault('max_price', 99999999.0)
    st.session_state.setdefault('load_more_requested', False)

    with st.sidebar:
        st.header("Filter Transactions")

        # LIKE filters
        st.text_input("Brokerage", key="filter_brokerage")
        st.text_input("Agent First Name", key="filter_agent_first")
        st.text_input("Agent Last Name", key="filter_agent_last")

        start_date, end_date = st.date_input(
            "List Date Range",
            value=default_range,
            key="date_range_input"
        )

        select_all_states = st.checkbox("Select All States", key="select_all_states", value=True)
        if select_all_states:
            selected_states = us_states
        else:
            selected_states = st.multiselect("State", us_states, default=us_states)

        selected_statuses = st.multiselect("Status", all_statuses, default=["Active"])

        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Min Price", min_value=0, max_value=99999999, value=0, step=10000)
            apply_filters = st.button("Apply Filters")  # Added here
        with col2:
            max_price = st.number_input("Max Price", min_value=0, max_value=99999999, value=99999999, step=10000)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Clear Filters"):
                st.session_state.transactions_offset = 0
                st.session_state.filtered_transactions_data = pd.DataFrame()
                st.session_state.total_matching_rows = 0
                st.rerun()

    st.session_state.date_range = (start_date, end_date)

    if apply_filters or st.session_state.filtered_transactions_data.empty or st.session_state.get("load_more_requested"):
        brokerage_filter = st.session_state.get("filter_brokerage", "").lower().strip()
        agent_first_filter = st.session_state.get("filter_agent_first", "").lower().strip()
        agent_last_filter = st.session_state.get("filter_agent_last", "").lower().strip()

        offset = st.session_state.transactions_offset
        st.session_state.selected_states = selected_states
        st.session_state.selected_statuses = selected_statuses
        st.session_state.min_price = min_price
        st.session_state.max_price = max_price

        df = load_transactions_data(
            limit=CACHE_LIMIT_TRANSACTIONS,
            offset=offset,
            date_range=st.session_state.date_range,
            states=selected_states,
            statuses=selected_statuses,
            price_min=min_price,
            price_max=max_price
        )

        if 'listing_agent_id' in df.columns:
            df['Price'] = df['Price'].astype(str)
            df['Price Numeric'] = df['Price'].str.replace(r'[\$,]', '', regex=True).astype(float)
            df['total_transaction_counts'] = df.groupby('listing_agent_id')['Agent MLS ID'].transform('count').fillna(1).astype(int)
            df['Avg. Listing Price Temp'] = df.groupby('listing_agent_id')['Price Numeric'].transform('mean')
            df['Avg. Listing Price'] = df.apply(
                lambda row: '${:,.2f}'.format(row['Avg. Listing Price Temp']) if pd.notna(row['Avg. Listing Price Temp']) and row['Avg. Listing Price Temp'] != 0
                else (f"${float(row['Price Numeric']):,.2f}" if 'Price Numeric' in row and pd.notna(row['Price Numeric']) else ""),
                axis=1
            )
            df.drop(columns=['Avg. Listing Price Temp'], inplace=True)
        else:
            df['total_transaction_counts'] = 1
            df['Avg. Listing Price'] = f"${float(df['Price'].iloc[0]):,.2f}" if not df.empty and pd.notna(df['Price'].iloc[0]) else ""

        if brokerage_filter:
            df = df[df["Brokerage"].str.lower().str.contains(brokerage_filter, na=False)]
        if agent_first_filter:
            df = df[df["Agent First"].str.lower().str.contains(agent_first_filter, na=False)]
        if agent_last_filter:
            df = df[df["Agent Last"].str.lower().str.contains(agent_last_filter, na=False)]

        if offset == 0:
            st.session_state.filtered_transactions_data = df
        else:
            st.session_state.filtered_transactions_data = pd.concat(
                [st.session_state.filtered_transactions_data, df], ignore_index=True
            )

        st.session_state.transactions_offset += CACHE_LIMIT_TRANSACTIONS
        st.session_state.total_matching_rows = get_total_matching_rows(
            date_range=st.session_state.date_range,
            states=selected_states,
            statuses=selected_statuses,
            price_min=min_price,
            price_max=max_price
        )
        st.session_state.load_more_requested = False

    df_display = st.session_state.filtered_transactions_data.iloc[
                 0: st.session_state.transactions_offset
                 ]

    if 'total_transaction_counts' not in df_display.columns:
        df_display['total_transaction_counts'] = (
            df_display.groupby('Agent MLS ID')['Agent MLS ID']
            .transform('count').fillna(1).astype(int)
            if 'Agent MLS ID' in df_display.columns
            else 1
        )

        if 'Avg. Listing Price' not in df_display.columns:
            if 'Agent MLS ID' in df_display.columns:
                df_display['Price'] = df_display['Price'].astype(str)
                df_display['Price Numeric'] = df_display['Price'].str.replace(r'[\$,]', '', regex=True).astype(float)
            df_display['Avg. Listing Price Temp'] = (
                df_display.groupby('Agent MLS ID')['Price Numeric']
                .transform('mean')
            )
            df_display['Avg. Listing Price'] = df_display.apply(
                lambda row: '${:,.2f}'.format(row['Avg. Listing Price Temp']) if pd.notna(row['Avg. Listing Price Temp']) and row['Avg. Listing Price Temp'] != 0
                else (f"${float(row['Price Numeric']):,.2f}" if 'Price Numeric' in row and pd.notna(row['Price Numeric']) else ""),
                axis=1
            )
            df_display.drop(columns=['Avg. Listing Price Temp'], inplace=True)
        else:
            df_display['Avg. Listing Price'] = f"${float(df_display['Price'].iloc[0]):,.2f}" if not df_display.empty and pd.notna(df_display['Price'].iloc[0]) else ""

    if not st.session_state.filtered_transactions_data.empty:
        df_display["Price"] = df_display["Price"].apply(format_price)
        st.dataframe(df_display[['Email', 'Agent First', 'Agent Last', 'Brokerage', 'List Date', 'Status', 'Price', 'Address 1', 'Address 2', 'City', 'State', 'Zip', 'SQFT', 'Phone', 'Agent MLS ID', 'Office ID', 'total_transaction_counts', 'Avg. Listing Price']], use_container_width=True)

        col_dl = st.columns([1])
        with col_dl[0]:
            if len(df_display) > 15000:
                os.makedirs(EXPORT_PATH, exist_ok=True)
                export_path = os.path.join(EXPORT_PATH, "filtered_transactions.csv")
                df_display.to_csv(export_path, index=False)
                with open(export_path, "rb") as f:
                    st.download_button(
                        label="Download Full CSV",
                        data=f,
                        file_name="filtered_transactions.csv",
                        mime="text/csv",
                        key="download_transactions_csv_large"
                    )
            else:
                csv_data = df_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Export Displayed Data as CSV",
                    data=csv_data,
                    file_name="filtered_transactions.csv",
                    mime="text/csv",
                    key="download_transactions_csv"
                )

        st.metric("Rows Displayed", len(df_display))
        st.metric("Total Rows Matching Filters", st.session_state.total_matching_rows)
        st.caption(f"Showing rows 1 – "
                   f"{len(df_display)} of "
                   f"{st.session_state.total_matching_rows}")

        if st.session_state.total_matching_rows > len(df_display):
            st.button("Load More", key="load_more_button")
            if st.session_state.load_more_requested:
                st.session_state.load_more_requested = False
                st.rerun()
    else:
        st.info("No transactions match the current filters.")
