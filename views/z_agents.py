import streamlit as st
from db import run_query  # Assuming db.py contains your run_query function
import pandas as pd
import numpy as np  # Import numpy for NaN checking

CACHE_LIMIT = 5000  # Number of rows per page

# --- Database Column Names (Verify these match your actual schema!) ---
DB_COL_SALES_LASTYEAR = '"sales_lastyear"'
DB_COL_AVG_VALUE = '"averageValueThreeYear"'
DB_COL_TEAM = '"Team"'
DB_COL_TEAM_ROLE = '"Team_role"'
DB_COL_ORG = '"Org"'
DB_COL_STATE = '"State"'
DB_TABLE_AGENTS = '"z_agents"'

# --- DataFrame Column Names (Expected names AFTER loading from DB) ---
DF_COL_SALES_LASTYEAR = 'sales_lastyear'
DF_COL_SALES_VALUE_CALCULATED = 'Sales $ (12 Mo.)'
DF_COL_AVG_VALUE_FROM_DB = 'averageValueThreeYear'

# --- Display Column Names (How you want them to appear in the UI) ---
DISPLAY_COL_SALES_NUMBER = 'Sales # (12 Mo.)'
DISPLAY_COL_SALES_VALUE = 'Sales $ (12 Mo.)'

# --- Slider Configuration (ADJUST THESE BASED ON YOUR DATA!) ---
SLIDER_SALES_NUM_MIN = 0
SLIDER_SALES_NUM_MAX = 100  # Use 100 for the slider, will display "99+"
SLIDER_SALES_NUM_STEP = 1
SLIDER_SALES_VAL_MIN = 0
SLIDER_SALES_VAL_MAX = 100_000_000  # 100M
SLIDER_SALES_VAL_STEP = 1_000_000
SLIDER_SALES_VAL_FORMAT = "$%d"

# Helper function for safe casting in SQL WHERE clauses
def sql_safe_cast(db_column_name, cast_type="bigint"):
    """Generates SQL fragment for safe casting, handling non-numeric chars and empty strings."""
    cleaned_col = f"NULLIF(REGEXP_REPLACE({db_column_name}::text, '[^0-9.]', '', 'g'), '')"
    return f"CAST({cleaned_col} AS {cast_type})"

# --- Updated Data Functions ---
def get_total_row_count(states=None, team_roles=None, active_teams=False,
                        sales_number_range=None, sales_value_range=None):
    """Calculates the total number of rows matching ALL filters, using ranges."""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required.")
        return 0

    where_clauses = [f"{DB_COL_TEAM} IS NOT NULL", f"{DB_COL_TEAM} <> ''"]
    params = {}

    if states:
        where_clauses.append(f"{DB_COL_STATE} IN %(states)s")
        params['states'] = tuple(states)
    if team_roles:
        where_clauses.append(f"{DB_COL_TEAM_ROLE} IN %(team_roles)s")
        params['team_roles'] = tuple(team_roles)
    if st.session_state.get("filter_brokerage", "").strip():
        where_clauses.append(f"{DB_COL_ORG} ILIKE %(brokerage)s")
        params['brokerage'] = f"%{st.session_state.filter_brokerage.strip()}%"

    # --- Filter by Sales Number Range ---
    sales_num_col_casted = sql_safe_cast(DB_COL_SALES_LASTYEAR, "bigint")
    if active_teams:
        where_clauses.append(f"{sales_num_col_casted} >= 1")

    # Apply slider range.  Important: Use 99 in the where clause, not 100.
    if sales_number_range and \
            (sales_number_range[0] != SLIDER_SALES_NUM_MIN or sales_number_range[1] != SLIDER_SALES_NUM_MAX):
        selected_min, selected_max = sales_number_range
        if selected_min > SLIDER_SALES_NUM_MIN:
            where_clauses.append(f"{sales_num_col_casted} >= %(sales_number_min)s")
            params['sales_number_min'] = selected_min
        # Max is 99 for filtering
        if selected_max < SLIDER_SALES_NUM_MAX:
            where_clauses.append(f"{sales_num_col_casted} <= %(sales_number_max)s")
            params['sales_number_max'] = selected_max

    # --- Filter by Sales Value Range ---
    sales_value_calculation = f"({sql_safe_cast(DB_COL_SALES_LASTYEAR, 'numeric')} * {sql_safe_cast(DB_COL_AVG_VALUE, 'numeric')})"
    if sales_value_range and \
            (sales_value_range[0] != SLIDER_SALES_VAL_MIN or sales_value_range[1] != SLIDER_SALES_VAL_MAX):
        selected_min, selected_max = sales_value_range
        if selected_min > SLIDER_SALES_VAL_MIN:
            where_clauses.append(f"{sales_value_calculation} >= %(sales_value_min)s")
            params['sales_value_min'] = selected_min
        if selected_max < SLIDER_SALES_VAL_MAX:
            where_clauses.append(f"{sales_value_calculation} <= %(sales_value_max)s")
            params['sales_value_max'] = selected_max

    where_clause = " AND ".join(where_clauses)
    query = f"SELECT COUNT(*) FROM {DB_TABLE_AGENTS} WHERE {where_clause}"

    print("--- Count Query ---")
    print(f"SQL: {query}")
    print(f"Params: {params}")
    print("-------------------")

    try:
        result = run_query(query, params=params)
        count = result.iloc[0][0] if not result.empty and result.iloc[0][0] is not None else 0
        print(f"Count Result: {count}")
        return count
    except Exception as e:
        st.error(f"Error executing count query: {e}")
        print(f"Error during count query execution: {e}")
        return 0




def load_data(limit=CACHE_LIMIT, offset=0, states=None, team_roles=None, active_teams=False,
              sales_number_range=None, sales_value_range=None):
    """Loads data from the database based on filters, using ranges."""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required.")
        return pd.DataFrame()

    where_clauses = [f"{DB_COL_TEAM} IS NOT NULL", f"{DB_COL_TEAM} <> ''"]
    params = {}

    if states:
        where_clauses.append(f"{DB_COL_STATE} IN %(states)s")
        params['states'] = tuple(states)
    if team_roles:
        where_clauses.append(f"{DB_COL_TEAM_ROLE} IN %(team_roles)s")
        params['team_roles'] = tuple(team_roles)
    if st.session_state.get("filter_brokerage", "").strip():
        where_clauses.append(f"{DB_COL_ORG} ILIKE %(brokerage)s")
        params['brokerage'] = f"%{st.session_state.filter_brokerage.strip()}%"

    # --- Filter by Sales Number Range ---
    sales_num_col_casted = sql_safe_cast(DB_COL_SALES_LASTYEAR, "bigint")
    if active_teams:
        where_clauses.append(f"{sales_num_col_casted} >= 1")

    # Apply slider range.  Important: Use 99 in the where clause, not 100.
    if sales_number_range and \
            (sales_number_range[0] != SLIDER_SALES_NUM_MIN or sales_number_range[1] != SLIDER_SALES_NUM_MAX):
        selected_min, selected_max = sales_number_range
        if selected_min > SLIDER_SALES_NUM_MIN:
            where_clauses.append(f"{sales_num_col_casted} >= %(sales_number_min)s")
            params['sales_number_min'] = selected_min
        # Max is 99 for filtering
        if selected_max < SLIDER_SALES_NUM_MAX:
            where_clauses.append(f"{sales_num_col_casted} <= %(sales_number_max)s")
            params['sales_number_max'] = selected_max

    # --- Filter by Sales Value Range ---
    sales_value_calculation = f"({sql_safe_cast(DB_COL_SALES_LASTYEAR, 'numeric')} * {sql_safe_cast(DB_COL_AVG_VALUE, 'numeric')})"
    if sales_value_range and \
            (sales_value_range[0] != SLIDER_SALES_VAL_MIN or sales_value_range[1] != SLIDER_SALES_VAL_MAX):
        selected_min, selected_max = sales_value_range
        if selected_min > SLIDER_SALES_VAL_MIN:
            where_clauses.append(f"{sales_value_calculation} >= %(sales_value_min)s")
            params['sales_value_min'] = selected_min
        if selected_max < SLIDER_SALES_VAL_MAX:
            where_clauses.append(f"{sales_value_calculation} <= %(sales_value_max)s")
            params['sales_value_max'] = selected_max

    where_clause = " AND ".join(where_clauses)

    query = f"""
    SELECT
        "Name", "Team", "Team_role", "Org", "Street", "City", "State", "Zip", "Office",
        "Phone", "Cell", "Email", "Website", "Facebook", "Linkedin",
        {DB_COL_SALES_LASTYEAR},
        {DB_COL_AVG_VALUE},
        "priceRangeThreeYearMin" AS "3 Year Min",
        "priceRangeThreeYearMax" AS "3 Year Max",
        {sales_value_calculation} AS "{DF_COL_SALES_VALUE_CALCULATED}"
    FROM {DB_TABLE_AGENTS}
    WHERE {where_clause}
    ORDER BY "Name"
    LIMIT %(limit)s OFFSET %(offset)s;
    """
    params['limit'] = limit
    params['offset'] = offset

    print("--- Data Query ---")
    print(f"SQL: {query}")
    print(f"Params: {params}")
    print("------------------")

    try:
        df = run_query(query, params=params)
        print(f"Columns returned by load_data: {df.columns.tolist()}")
        if not df.empty:
            print("Raw data sample (head) from load_data:")
            cols_to_print = [col for col in
                             [DF_COL_SALES_LASTYEAR, DF_COL_AVG_VALUE_FROM_DB, DF_COL_SALES_VALUE_CALCULATED] if
                             col in df.columns]
            if cols_to_print:
                print(df[cols_to_print].head())
            else:
                print("Could not find expected sales/average columns in returned data.")

        if DF_COL_SALES_LASTYEAR not in df.columns:
            print(f"Warning: Expected DataFrame column '{DF_COL_SALES_LASTYEAR}' not found after query.")
            df[DF_COL_SALES_LASTYEAR] = np.nan
        if DF_COL_SALES_VALUE_CALCULATED not in df.columns:
            print(
                f"Warning: Expected DataFrame column '{DF_COL_SALES_VALUE_CALCULATED}' not found after query.")
            df[DF_COL_SALES_VALUE_CALCULATED] = np.nan
        print(f"Number of rows returned by load_data: {len(df)}")
        return df
    except Exception as e:
        st.error(f"Error executing data query: {e}")
        print(f"Error during data query execution: {e}")
        return pd.DataFrame()


def z_agents_view():
    """Displays the Z Agents data view with filtering and pagination."""
    st.title("Team Members View")

    # Initialize session state variables safely
    st.session_state.setdefault('offset', 0)
    st.session_state.setdefault('filtered_data', pd.DataFrame())
    st.session_state.setdefault('total_rows', 0)
    st.session_state.setdefault('selected_states', [])
    st.session_state.setdefault('selected_team_roles', [])
    st.session_state.setdefault('active_teams_only', True)
    st.session_state.setdefault('sales_number_range', (SLIDER_SALES_NUM_MIN, SLIDER_SALES_NUM_MAX))
    st.session_state.setdefault('sales_value_range', (SLIDER_SALES_VAL_MIN, SLIDER_SALES_VAL_MAX))
    st.session_state.setdefault('authenticated', True)
    st.session_state.setdefault('filter_brokerage', '')

    if st.session_state.get('authenticated', False):
        # --- Sidebar Filters ---
        with st.sidebar:
            st.header("Filter by Location and Role")
            us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                         'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                         'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                         'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                         'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

            select_all_states = st.checkbox("Select All States", key="select_all_states", value=True)
            if select_all_states:
                st.session_state.selected_states = us_states
            else:
                st.session_state.selected_states = st.multiselect(
                    "State", us_states, default=st.session_state.selected_states, key="state_select"
                )
            st.text_input("Brokerage", key="filter_brokerage")

            try:
                role_query = f"SELECT DISTINCT {DB_COL_TEAM_ROLE} FROM {DB_TABLE_AGENTS} WHERE {DB_COL_TEAM_ROLE} IS NOT NULL AND {DB_COL_TEAM_ROLE} <> '' ORDER BY {DB_COL_TEAM_ROLE}"
                print(f"Executing Team Role Query: {role_query}")
                unique_team_roles_df = run_query(role_query)
                unique_team_roles = sorted(unique_team_roles_df[unique_team_roles_df.columns[0]].dropna().unique()) if not unique_team_roles_df.empty else []
            except Exception as e:
                st.warning(f"Could not load team roles: {e}")
                print(f"Error loading team roles: {e}")
                unique_team_roles = []

            st.session_state.selected_team_roles = st.multiselect(
                "Team Role", unique_team_roles, default=st.session_state.selected_team_roles, key="role_select"
            )

            st.subheader("Sales Filters")
            st.session_state.active_teams_only = st.checkbox(
                "Active Members Only", value=st.session_state.active_teams_only,
                key="active_teams_check"
            )

            # --- Sales Number Slider ---
            # Important:  The max slider value is 100, but the actual filter in the query uses 99.
            st.session_state.sales_number_range = st.slider(
                "Filter by Sales # (12 Mo.)",
                min_value=SLIDER_SALES_NUM_MIN,
                max_value=SLIDER_SALES_NUM_MAX,  # Use 100 here
                value=st.session_state.sales_number_range,
                step=SLIDER_SALES_NUM_STEP,
                format="%d",  # No formatting for the slider, show raw numbers
                key="sales_num_slider"
            )

            st.session_state.sales_value_range = st.slider(
                "Filter by Sales $ (12 Mo.)",
                min_value=SLIDER_SALES_VAL_MIN,
                max_value=SLIDER_SALES_VAL_MAX,
                value=st.session_state.sales_value_range,
                step=SLIDER_SALES_VAL_STEP,
                format=SLIDER_SALES_VAL_FORMAT,
                key="sales_val_slider"
            )

            col1, col2 = st.columns(2)
            with col1:
                apply_filters_button = st.button("Apply Filters", key="apply_filters")
            with col2:
                if st.button("Clear Filters", key="clear_filters"):
                    # Reset filter-related session state values to defaults
                    st.session_state.selected_states = []
                    st.session_state.selected_team_roles = []
                    st.session_state.active_teams_only = True
                    st.session_state.sales_number_range = (SLIDER_SALES_NUM_MIN, SLIDER_SALES_NUM_MAX)
                    st.session_state.sales_value_range = (SLIDER_SALES_VAL_MIN, SLIDER_SALES_VAL_MAX)
                    if "filter_brokerage" in st.session_state:
                        del st.session_state["filter_brokerage"]
                    st.session_state.offset = 0
                    st.session_state.filtered_data = pd.DataFrame()
                    st.rerun()

        if 'auto_loaded' not in st.session_state:
            with st.spinner("Loading data..."):
                st.session_state.total_rows = get_total_row_count(
                    st.session_state.selected_states,
                    st.session_state.selected_team_roles,
                    st.session_state.active_teams_only,
                    st.session_state.sales_number_range,
                    st.session_state.sales_value_range
                )
                st.session_state.filtered_data = load_data(
                    CACHE_LIMIT, 0,
                    st.session_state.selected_states,
                    st.session_state.selected_team_roles,
                    st.session_state.active_teams_only,
                    st.session_state.sales_number_range,
                    st.session_state.sales_value_range
                )
            st.session_state.auto_loaded = True
            st.session_state.filters_applied = True
            st.rerun()
        # --- Apply Filters Logic ---
        if apply_filters_button:
            st.session_state.offset = 0
            print("\n--- Applying Filters ---")
            with st.spinner("Calculating total rows..."):
                st.session_state.total_rows = get_total_row_count(
                    st.session_state.selected_states, st.session_state.selected_team_roles,
                    st.session_state.active_teams_only,
                    st.session_state.sales_number_range,
                    st.session_state.sales_value_range
                )
            if st.session_state.total_rows > 0:
                with st.spinner("Loading data..."):
                    st.session_state.filtered_data = load_data(
                        CACHE_LIMIT, 0, st.session_state.selected_states, st.session_state.selected_team_roles,
                        st.session_state.active_teams_only,
                        st.session_state.sales_number_range,
                        st.session_state.sales_value_range
                    )
            else:
                st.session_state.filtered_data = pd.DataFrame()
            print("------------------------\n")
            st.rerun()
            if st.session_state.filtered_data.empty and 'preloaded' not in st.session_state:
                st.session_state.total_rows = get_total_row_count(
                    st.session_state.selected_states,
                    st.session_state.selected_team_roles,
                    st.session_state.active_teams_only,
                    st.session_state.sales_number_range,
                    st.session_state.sales_value_range
                )
                st.session_state.filtered_data = load_data(
                    CACHE_LIMIT, 0,
                    st.session_state.selected_states,
                    st.session_state.selected_team_roles,
                    st.session_state.active_teams_only,
                    st.session_state.sales_number_range,
                    st.session_state.sales_value_range
                )
                st.session_state.preloaded = True

        # --- Display Data ---
        current_data = st.session_state.get('filtered_data', pd.DataFrame())

        if not current_data.empty:
            df_display = current_data.copy()
            print(f"Columns in df_display before formatting: {df_display.columns.tolist()}")

            # --- Format Sales Columns for Display ---
            df_display[DISPLAY_COL_SALES_NUMBER] = df_display[DF_COL_SALES_LASTYEAR]  # Show actual value
            df_display[DISPLAY_COL_SALES_VALUE] = df_display[DF_COL_SALES_VALUE_CALCULATED] # show calculated

            #  Format for display in the table
            df_display[DISPLAY_COL_SALES_NUMBER] = df_display[DISPLAY_COL_SALES_NUMBER].apply(lambda x: f"{x:,.0f}")
            df_display[DISPLAY_COL_SALES_VALUE] = df_display[DISPLAY_COL_SALES_VALUE].apply(lambda x: f"${x:,.0f}")

            # Optionally format the new price range columns if needed (here showing two decimal places)
            df_display["3 Year Min"] = df_display["3 Year Min"].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else x)
            df_display["3 Year Max"] = df_display["3 Year Max"].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else x)

            # --- Define Columns for Display ---
            columns_to_display_in_table = [
                "Name", "Team", "Team_role", "Org",
                "Phone", "Cell", "Email",
                DISPLAY_COL_SALES_NUMBER,
                DISPLAY_COL_SALES_VALUE,
                "3 Year Min",
                "3 Year Max"
            ]
            valid_columns_to_display = [col for col in columns_to_display_in_table if col in df_display.columns]

            if not valid_columns_to_display:
                st.error("No valid columns available to display.")
            else:
                # --- Display DataFrame ---
                st.dataframe(
                    df_display[valid_columns_to_display],
                    use_container_width=True,
                    column_config={
                        # Email defaults to text display now
                        DISPLAY_COL_SALES_NUMBER: st.column_config.TextColumn(help=f"Source: {DF_COL_SALES_LASTYEAR}"),
                        DISPLAY_COL_SALES_VALUE: st.column_config.TextColumn(
                            help=f"Source: {DF_COL_SALES_VALUE_CALCULATED}"),
                    },
                    hide_index=True
                )

            # --- Metrics and Download ---
            col_metric, col_dl = st.columns([2, 1])
            with col_metric:
                st.metric("Rows Displayed", len(df_display))
                st.metric("Total Rows Matching Filters", st.session_state.total_rows)
                start_row = st.session_state.offset + 1
                end_row = st.session_state.offset + len(df_display)
                st.caption(f"Showing rows {start_row}-{end_row} of {st.session_state.total_rows}")
            with col_dl:
                csv_data = df_display[valid_columns_to_display].to_csv(index=False).encode('utf-8')
                st.download_button(label="Export Displayed Data as CSV", data=csv_data,
                                   file_name="filtered_agents_view.csv", mime="text/csv", key='download_csv')

            # --- Pagination ---
            if end_row < st.session_state.total_rows:
                if st.button("Load More", key="load_more"):
                    print("\n--- Loading More ---")
                    with st.spinner("Loading more data..."):
                        new_data = load_data(
                            CACHE_LIMIT, st.session_state.offset + CACHE_LIMIT,
                            st.session_state.selected_states, st.session_state.selected_team_roles,
                            st.session_state.active_teams_only,
                            st.session_state.sales_number_range,
                            st.session_state.sales_value_range
                        )
                    if not new_data.empty:
                        st.session_state.filtered_data = pd.concat(
                            [st.session_state.filtered_data, new_data], ignore_index=True
                        )
                        st.session_state.offset += CACHE_LIMIT
                        print("------------------\n")
                        st.rerun()
                    else:
                        st.warning("No more data found.")
                        print("Load More returned empty DataFrame unexpectedly.")
            elif st.session_state.total_rows > 0 and not current_data.empty:
                st.success("All matching data loaded.")

        # --- Handle No Data Scenarios ---
        elif st.session_state.total_rows == 0 and st.session_state.get('filters_applied', False):
            st.info("No data matches the current filters.")
        elif not st.session_state.get('filters_applied', False):
            st.info("Apply filters using the sidebar to load data.")

    else:
        st.info("Please log in to view Z Agents data.")


# --- Main execution ---
if __name__ == "__main__":
    print("----- Streamlit App Start / Rerun -----")
    z_agents_view()
