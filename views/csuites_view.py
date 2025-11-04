import streamlit as st
import pandas as pd
import os
import re
import snowflake.connector
from snowflake.connector import DictCursor
# Assuming config is available
try:
    from config import EXPORT_PATH
except ImportError:
    # Define a default if config isn't available for local testing
    EXPORT_PATH = 'exports' 

# --- Connection and Query Helpers ---

def connect_snowflake():
    import streamlit as st
    creds = st.secrets

    return snowflake.connector.connect(
        user=creds["SNOWFLAKE_USER"],
        password=creds["SNOWFLAKE_PASSWORD"],
        account=creds["SNOWFLAKE_ACCOUNT"],
        warehouse=creds["SNOWFLAKE_WAREHOUSE"],
        database=creds["SNOWFLAKE_DATABASE"],
        schema=creds["SNOWFLAKE_SCHEMA"],
        role=creds["SNOWFLAKE_ROLE"],
    )
    

def run_snowflake_query(query, params=None):
    """Executes a query on Snowflake and returns a DataFrame."""
    # Normalize any Unicode comparison operators that may sneak in
    sql = query.replace("≥", ">=").replace("≤", "<=")
    # Convert SQLAlchemy/colon binds (:name) to pyformat binds (%(name)s)
    if params:
        sql = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"%(\1)s", sql)
    conn = connect_snowflake()
    cur = None
    try:
        cur = conn.cursor(DictCursor)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        # DictCursor returns list[dict] with correct column names
        df = pd.DataFrame(rows)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            conn.close()


# --- Data Loading Function (Agents Count Logic Applied) ---

def load_csuites_data(
    name_filter=None, company_filter=None,
    title_filter=None, job_function_filter=None,
    city_filter=None, state_filter=None,
    agents_count_min=None, agents_count_max=None,
    show_all_records=False
):
    """Loads all C-Suite data from Snowflake based on filters (no LIMIT/OFFSET)."""
    where_clauses = []
    params = {}
    
    # Define the default max from the Streamlit input for comparison
    DEFAULT_MAX_AGENTS = 1_000_000

    if name_filter:
        where_clauses.append('(FIRST_NAME ILIKE :name_like OR LAST_NAME ILIKE :name_like)')
        params['name_like'] = f"%{name_filter}%"

    if company_filter:
        where_clauses.append('COMPANY ILIKE :company_like')
        params['company_like'] = f"%{company_filter}%"

    if title_filter:
        where_clauses.append('TITLE ILIKE :title_like')
        params['title_like'] = f"%{title_filter}%"

    if job_function_filter:
        where_clauses.append('JOB_FUNCTION = :job_function')
        params['job_function'] = job_function_filter

    if city_filter:
        where_clauses.append('LOWER(COMPANY_CITY) LIKE :city_like')
        params['city_like'] = f"%{city_filter}%"

    if state_filter:
        # state_filter is expected to be a list of states
        placeholders = []
        for i, state in enumerate(state_filter):
            key = f"state_{i}"
            placeholders.append(f":{key}")
            params[key] = state
        where_clauses.append(f"COMPANY_STATE IN ({', '.join(placeholders)})")

    # MODIFIED LOGIC: Only apply min filter if it's set above the default of 0
    if agents_count_min is not None and agents_count_min > 0:
        # Include NULLs if the filter is applied
        where_clauses.append('(AGENTS_COUNT >= :agents_count_min OR AGENTS_COUNT IS NULL)')
        params['agents_count_min'] = agents_count_min

    # MODIFIED LOGIC: Only apply max filter if it's set below the default max
    if agents_count_max is not None and agents_count_max < DEFAULT_MAX_AGENTS:
        # Include NULLs if the filter is applied
        where_clauses.append('(AGENTS_COUNT <= :agents_count_max OR AGENTS_COUNT IS NULL)')
        params['agents_count_max'] = agents_count_max

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    limit_clause = "" if show_all_records else "LIMIT 1000"

    query = f"""
    SELECT
        COMPANY AS "Brokerage",
        FIRST_NAME AS "First Name",
        LAST_NAME AS "Last Name",
        TITLE AS "Title",
        JOB_FUNCTION AS "Job Function",
        PHONE AS "Phone",
        EMAIL AS "Email",
        COMPANY_ADDRESS AS "Address",
        COMPANY_CITY AS "City",
        COMPANY_STATE AS "State",
        COMPANY_POSTAL_CODE AS "Zip Code",
        COMPANY_PHONE AS "Office Phone",
        COMPANY_WEBSITE AS "Website",
        AGENTS_COUNT AS "Agent Count"
    FROM SCOUT_DW.COMPCURVE.CSUITES
    {where_clause}
    ORDER BY AGENTS_COUNT DESC NULLS LAST
    {limit_clause}
    """

    df = run_snowflake_query(query, params=params)
    return df


# --- Total Count Function (Agents Count Logic Applied) ---

def get_total_csuites_count(name_filter=None, company_filter=None,
                            title_filter=None, job_function_filter=None,
                            city_filter=None, state_filter=None,
                            agents_count_min=None, agents_count_max=None):
    """Counts total number of C-Suite records matching the filters."""
    where_clauses = []
    params = {}
    
    # Define the default max from the Streamlit input for comparison
    DEFAULT_MAX_AGENTS = 1_000_000

    if name_filter:
        where_clauses.append('(FIRST_NAME ILIKE :name_like OR LAST_NAME ILIKE :name_like)')
        params['name_like'] = f"%{name_filter}%"

    if company_filter:
        where_clauses.append('COMPANY ILIKE :company_like')
        params['company_like'] = f"%{company_filter}%"

    if title_filter:
        where_clauses.append('TITLE ILIKE :title_like')
        params['title_like'] = f"%{title_filter}%"

    if job_function_filter:
        where_clauses.append('JOB_FUNCTION = :job_function')
        params['job_function'] = job_function_filter

    if city_filter:
        where_clauses.append('LOWER(COMPANY_CITY) LIKE :city_like')
        params['city_like'] = f"%{city_filter}%"

    if state_filter:
        placeholders = []
        for i, state in enumerate(state_filter):
            key = f"state_{i}"
            placeholders.append(f":{key}")
            params[key] = state
        where_clauses.append(f"COMPANY_STATE IN ({', '.join(placeholders)})")

    # MODIFIED LOGIC: Only apply min filter if it's set above the default of 0
    if agents_count_min is not None and agents_count_min > 0:
        # Include NULLs if the filter is applied
        where_clauses.append('(AGENTS_COUNT >= :agents_count_min OR AGENTS_COUNT IS NULL)')
        params['agents_count_min'] = agents_count_min

    # MODIFIED LOGIC: Only apply max filter if it's set below the default max
    if agents_count_max is not None and agents_count_max < DEFAULT_MAX_AGENTS:
        # Include NULLs if the filter is applied
        where_clauses.append('(AGENTS_COUNT <= :agents_count_max OR AGENTS_COUNT IS NULL)')
        params['agents_count_max'] = agents_count_max

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
    SELECT COUNT(*) AS "total"
    FROM SCOUT_DW.COMPCURVE.CSUITES
    {where_clause}
    """

    df = run_snowflake_query(query, params=params)
    if not df.empty:
        # Prefer exact lowercase alias preserved via quotes
        if 'total' in df.columns:
            val = df.iloc[0]['total']
        # Fallbacks for safety (some drivers may still upper-case keys)
        elif 'TOTAL' in df.columns:
            val = df.iloc[0]['TOTAL']
        else:
            # Final fallback: take the first column's value
            val = df.iloc[0].iloc[0]
        try:
            return int(val if val is not None else 0)
        except Exception:
            return 0
    return 0


# --- Streamlit View Function ---

def csuites_view():
    """Main view for C-Suite data from Snowflake."""
    st.title("C-Suite Executives")

    # Calculate the total count of all records in the table (unfiltered)
    # Cache this value to avoid unnecessary re-queries on every interaction
    @st.cache_data(ttl=3600)
    def get_all_records_count():
        # Get count without any filters
        return get_total_csuites_count()

    total_unfiltered_count = get_all_records_count()
    st.info(f"The total number of all C-Suite records in the database is: **{total_unfiltered_count:,}**")

    # Filters in sidebar
    with st.sidebar:
        st.markdown("### Filters")

        st.text_input(
            "Name",
            key="filter_csuite_name",
            placeholder="Search by first or last name..."
        )
        st.text_input(
            "Company",
            key="filter_csuite_company",
            placeholder="Search by company..."
        )
        st.text_input(
            "Title",
            key="filter_csuite_title",
            placeholder="Search by title..."
        )
        job_function_options = [
            "Leadership & Ownership",
            "Sales & Agent Functions",
            "Operations & Administration",
            "Marketing & Lead Generation",
            "Finance & Back Office",
            "Technology & Data",
            "HR & Talent",
            "Mortgage, Title & Related",
            "Property & Asset Management",
            "Coaching & Training",
            "Support & Client Services"
        ]
        job_function_filter = st.selectbox(
            "Job Function",
            options=[""] + job_function_options,
            index=0,
            key="filter_csuite_job_function"
        )
        st.text_input(
            "City",
            key="filter_csuite_city",
            placeholder="Search by city..."
        )
        state_options = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        ]

        # Ensure defaults are valid
        saved_state_filter = st.session_state.get("filter_csuite_state", [])
        valid_default = [s for s in saved_state_filter if s in state_options]

        state_filter = st.multiselect(
            "State",
            options=state_options,
            default=valid_default,
            key="filter_csuite_state"
        )
        show_all_records = st.checkbox("Show all records (70K+)", value=False, key="show_all_records")
        st.markdown("---")
        
        # Agents Count Range defaults
        DEFAULT_MIN_AGENTS = 0
        DEFAULT_MAX_AGENTS = 1_000_000
        
        with st.expander("Agent Count Range", expanded=False):
            agents_col1, agents_col2 = st.columns(2)
            with agents_col1:
                agents_count_min = st.number_input(
                    "Min Agents",
                    min_value=0,
                    max_value=DEFAULT_MAX_AGENTS,
                    value=st.session_state.get("agents_count_min", DEFAULT_MIN_AGENTS),
                    step=1,
                    key="agents_count_min"
                )
            with agents_col2:
                agents_count_max = st.number_input(
                    "Max Agents",
                    min_value=0,
                    max_value=DEFAULT_MAX_AGENTS,
                    value=st.session_state.get("agents_count_max", DEFAULT_MAX_AGENTS),
                    step=1,
                    key="agents_count_max"
                )

    # Retrieve filters from session state
    name_filter = st.session_state.get("filter_csuite_name", "").strip()
    company_filter = st.session_state.get("filter_csuite_company", "").strip()
    title_filter = st.session_state.get("filter_csuite_title", "").strip()
    job_function_filter = st.session_state.get("filter_csuite_job_function", "")
    if job_function_filter == "":
        job_function_filter = None
    city_filter = st.session_state.get("filter_csuite_city", "").strip().lower()
    state_filter = st.session_state.get("filter_csuite_state", [])
    agents_count_min = st.session_state.get("agents_count_min", DEFAULT_MIN_AGENTS)
    agents_count_max = st.session_state.get("agents_count_max", DEFAULT_MAX_AGENTS)
    show_all_records = st.session_state.get("show_all_records", False)

    # Compose current filters as a dict for comparison
    current_filters = {
        "name_filter": name_filter,
        "company_filter": company_filter,
        "title_filter": title_filter,
        "job_function_filter": job_function_filter,
        "city_filter": city_filter,
        "state_filter": state_filter,
        "agents_count_min": agents_count_min,
        "agents_count_max": agents_count_max,
        "show_all_records": show_all_records,
    }

    # Use session state to avoid unnecessary reloads, but always load all filtered data
    if 'csuites_last_filters' not in st.session_state or current_filters != st.session_state['csuites_last_filters']:
        with st.spinner("Loading C-Suite data..."):
            df_csuites = load_csuites_data(
                name_filter=name_filter,
                company_filter=company_filter,
                title_filter=title_filter,
                job_function_filter=job_function_filter,
                city_filter=city_filter,
                state_filter=state_filter,
                agents_count_min=agents_count_min,
                agents_count_max=agents_count_max,
                show_all_records=show_all_records
            )
        st.session_state['csuites_df'] = df_csuites
        st.session_state['csuites_last_filters'] = current_filters.copy()
    else:
        df_csuites = st.session_state.get('csuites_df', pd.DataFrame())

    # Calculate total count for metrics using the dedicated function
    total_csuites = get_total_csuites_count(
        name_filter=name_filter,
        company_filter=company_filter,
        title_filter=title_filter,
        job_function_filter=job_function_filter,
        city_filter=city_filter,
        state_filter=state_filter,
        agents_count_min=agents_count_min,
        agents_count_max=agents_count_max
    )

    if not df_csuites.empty:
        st.dataframe(df_csuites, use_container_width=True, hide_index=True, height=800) # Reduced height for better visibility
        st.caption(f"Loaded {len(df_csuites)} records from Snowflake (up to 1,000 for display, or all if checked).")

        col_metric_csuites, col_dl_csuites = st.columns([2, 1])
        with col_metric_csuites:
            st.metric("Rows Displayed", len(df_csuites))
            st.metric("Total Rows Matching Filters", total_csuites)
            start = 1
            end = len(df_csuites)
            st.caption(f"Showing records {start}-{end} of {total_csuites} filtered records.")

        with col_dl_csuites:
            if total_csuites > 15000:
                os.makedirs(EXPORT_PATH, exist_ok=True)
                export_path = os.path.join(EXPORT_PATH, "csuites_view_full.csv")
                # To get the *full* data for download, we should re-query without LIMIT,
                # unless show_all_records was already checked and the current df_csuites is the full set.
                if not show_all_records:
                    st.warning("Re-loading all records for download...")
                    df_csuites_full = load_csuites_data(
                        name_filter=name_filter, company_filter=company_filter,
                        title_filter=title_filter, job_function_filter=job_function_filter,
                        city_filter=city_filter, state_filter=state_filter,
                        agents_count_min=agents_count_min, agents_count_max=agents_count_max,
                        show_all_records=True # Ensure full set is loaded
                    )
                else:
                    df_csuites_full = df_csuites

                df_csuites_full.to_csv(export_path, index=False)
                with open(export_path, "rb") as f:
                    st.download_button(
                        label="Download Full CSV",
                        data=f,
                        file_name="csuites_view_full.csv",
                        mime="text/csv",
                        key="download_csuites_csv_large"
                    )
            else:
                csv_data = df_csuites.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Export Full Data as CSV",
                    data=csv_data,
                    file_name="csuites_view_full.csv",
                    mime="text/csv",
                    key="download_csuites_csv"
                )
    else:
        st.info("No C-Suite records match the current filters.")
