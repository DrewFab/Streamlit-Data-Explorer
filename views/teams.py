import streamlit as st
import pandas as pd
import numpy as np
from db import run_query  # Assuming db.py is in the same directory or path is configured

CACHE_LIMIT_TEAMS = 5000  # Number of rows per page for teams

# --- Database Column Names for Teams (Verify these match your actual schema!) ---
DB_COL_TEAM_NAME = '"Team Name"'
DB_COL_TEAM_LEAD = '"Team Lead"'
DB_COL_TEAM_MEMBERS_COUNT = '"Team Members"'
DB_COL_BROKERAGE = '"Brokerage"'
DB_COL_STATE_TEAMS = '"State"'
DB_COL_TOTAL_SALES = '"Total Sales"'
DB_COL_SALES_LASTYEAR_TEAMS = '"Sales 12 Mo."'
DB_COL_AVG_SALE_TEAMS = '"Avg. Sale"'
DB_COL_MEMBERS_LIST = '"Members"'
DB_TABLE_TEAMS = '"z_agents"'  # Assuming the same table for now, adjust if needed

# --- DataFrame Column Names for Teams ---
DF_COL_TEAM_NAME = 'Team Name'
DF_COL_TEAM_LEAD = 'Team Lead'
DF_COL_TEAM_MEMBERS_COUNT = 'Team Members'
DF_COL_BROKERAGE = 'Brokerage'
DF_COL_STATE_TEAMS = 'State'
DF_COL_TOTAL_SALES = 'Total Sales'
DF_COL_SALES_LASTYEAR_TEAMS = 'Sales 12 Mo.'
DF_COL_AVG_SALE_TEAMS = 'Avg. Sale'
DF_COL_MEMBERS_LIST = 'Members'

# --- Display Column Names for Teams ---
DISPLAY_COL_TEAM_NAME = 'Team Name'
DISPLAY_COL_TEAM_LEAD = 'Team Lead'
DISPLAY_COL_TEAM_MEMBERS_COUNT = 'Team Members'
DISPLAY_COL_BROKERAGE = 'Brokerage'
DISPLAY_COL_STATE_TEAMS = 'State'
DISPLAY_COL_TOTAL_SALES = 'Total Sales'
DISPLAY_COL_SALES_LASTYEAR_TEAMS = 'Sales 12 Mo.'
DISPLAY_COL_AVG_SALE_TEAMS = 'Avg. Sale'
DISPLAY_COL_MEMBERS_LIST = 'Members'


def get_total_team_count(states=None):
    """Calculates the total number of teams matching the filters."""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required.")
        return 0

    where_clauses = [
        'lead."Team_role" = \'Lead\'',
        'lead."Team_encodedZuid" IS NOT NULL'
    ]
    params = {}

    # Apply filter for state(s)
    if states:
        where_clauses.append('lead."State" IN %(states)s')
        params['states'] = tuple(states)

    # Additional filter for Brokerage
    if st.session_state.get("filter_brokerage", "").strip():
        where_clauses.append('lead."Org" ILIKE %(brokerage)s')
        params['brokerage'] = f"%{st.session_state.filter_brokerage.strip()}%"

    # Additional filter for Team
    if st.session_state.get("filter_team", "").strip():
        where_clauses.append('lead."Team" ILIKE %(team)s')
        params['team'] = f"%{st.session_state.filter_team.strip()}%"

    # Additional filter for Sales 12 Mo.
    if st.session_state.get("filter_sales12"):
        sales12_range = st.session_state.filter_sales12
        if sales12_range != (0, 100):
            where_clauses.append('lead."sales_lastyear" BETWEEN %(sales12_min)s AND %(sales12_max)s')
            params['sales12_min'] = sales12_range[0]
            params['sales12_max'] = sales12_range[1]

    where_clause = " AND ".join(where_clauses)
    query = f"""
    SELECT COUNT(DISTINCT lead."Team_encodedZuid")
    FROM {DB_TABLE_TEAMS} AS lead
    WHERE {where_clause};
    """

    print("--- Teams Count Query ---")
    print(f"SQL: {query}")
    print(f"Params: {params}")
    print("-------------------------")

    try:
        result = run_query(query, params=params)
        count = result.iloc[0][0] if not result.empty and result.iloc[0][0] is not None else 0
        print(f"Teams Count Result: {count}")
        return count
    except Exception as e:
        st.error(f"Error executing teams count query: {e}")
        print(f"Error during teams count query execution: {e}")
        return 0


def load_team_data(limit=CACHE_LIMIT_TEAMS, offset=0, states=None):
    """Loads team data from the database based on filters."""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required.")
        return pd.DataFrame()

    where_clauses = [
        'lead."Team_role" = \'Lead\'',
        'lead."Team_encodedZuid" IS NOT NULL'
    ]
    params = {'limit': limit, 'offset': offset}

    # Apply filter for state(s)
    if states:
        where_clauses.append('lead."State" IN %(states)s')
        params['states'] = tuple(states)

    # Additional filter for Brokerage
    if st.session_state.get("filter_brokerage", "").strip():
        where_clauses.append('lead."Org" ILIKE %(brokerage)s')
        params['brokerage'] = f"%{st.session_state.filter_brokerage.strip()}%"

    # Additional filter for Team
    if st.session_state.get("filter_team", "").strip():
        where_clauses.append('lead."Team" ILIKE %(team)s')
        params['team'] = f"%{st.session_state.filter_team.strip()}%"

    # Additional filter for Sales 12 Mo.
    if st.session_state.get("filter_sales12"):
        sales12_range = st.session_state.filter_sales12
        if sales12_range != (0, 100):
            where_clauses.append('lead."sales_lastyear" BETWEEN %(sales12_min)s AND %(sales12_max)s')
            params['sales12_min'] = sales12_range[0]
            params['sales12_max'] = sales12_range[1]

    where_clause = " AND ".join(where_clauses)
    query = f"""
    SELECT
        lead."Team" AS "{DF_COL_TEAM_NAME}",
        lead."Name" AS "{DF_COL_TEAM_LEAD}",
        COUNT(team_records."Team_encodedZuid") AS "{DF_COL_TEAM_MEMBERS_COUNT}",
        lead."Org" AS "{DF_COL_BROKERAGE}",
        lead."State" AS "{DF_COL_STATE_TEAMS}",
        lead."sales" AS "{DF_COL_TOTAL_SALES}",
        lead."sales_lastyear" AS "{DF_COL_SALES_LASTYEAR_TEAMS}",
        lead."averageValueThreeYear" AS "{DF_COL_AVG_SALE_TEAMS}",
        REPLACE(lead."Team_member_name(cut by ^)", '^', '; ') AS "{DF_COL_MEMBERS_LIST}"
    FROM {DB_TABLE_TEAMS} AS lead
    JOIN {DB_TABLE_TEAMS} AS team_records
        ON lead."Team_encodedZuid" = team_records."Team_encodedZuid"
    WHERE {where_clause}
    GROUP BY lead."Team", lead."Name", lead."Team_encodedZuid", lead."Org", lead."State",
             lead."sales", lead."sales_lastyear", lead."averageValueThreeYear",
             lead."Team_member_name(cut by ^)"
    ORDER BY lead."State" ASC
    LIMIT %(limit)s OFFSET %(offset)s;
    """

    print("--- Teams Data Query ---")
    print(f"SQL: {query}")
    print(f"Params: {params}")
    print("------------------------")

    try:
        df = run_query(query, params=params)
        print(f"Columns returned by load_team_data: {df.columns.tolist()}")
        if not df.empty:
            print("Raw team data sample (head) from load_team_data:")
            print(df.head())
        return df
    except Exception as e:
        st.error(f"Error executing teams data query: {e}")
        print(f"Error during teams data query execution: {e}")
        return pd.DataFrame()


def teams_view():
    """Displays the Teams data view with filtering and pagination."""
    st.title("Teams View")

    us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    st.session_state.setdefault('selected_states_teams', us_states)

    # Initialize session state variables safely for teams view.
    # Note: Using a different key for our flag than the widget key.
    st.session_state.setdefault('teams_offset', 0)
    st.session_state.setdefault('filtered_teams_data', pd.DataFrame())
    st.session_state.setdefault('total_teams', 0)
    st.session_state.setdefault('teams_filters_applied', False)  # Renamed flag

    if st.session_state.get('authenticated', False):
        # --- Sidebar Filters for Teams ---
        with st.sidebar:
            st.header("Filter Teams by Location")
            select_all_states_teams = st.checkbox("Select All States", key="select_all_states_teams", value=True)
            if select_all_states_teams:
                st.session_state.selected_states_teams = us_states
            else:
                st.session_state.selected_states_teams = st.multiselect(
                    "State", us_states, default=st.session_state.selected_states_teams, key="state_select_teams"
                )

            # New filters for Brokerage, Team, and Sales 12 Mo.
            st.text_input("Brokerage", key="filter_brokerage")
            st.text_input("Team", key="filter_team")
            st.slider("Sales 12 Mo.", min_value=0, max_value=100, value=(0, 100), step=1, key="filter_sales12")

            # Use a unique key for the button to avoid conflicts with session_state flag
            col1, col2 = st.columns(2)
            with col1:
                apply_filters_btn = st.button("Apply Filters", key="apply_filters_btn")
            with col2:
                if st.button("Clear Filters", key="clear_filters"):
                    st.session_state.selected_states_teams = us_states
                    # Remove widget keys from session_state to clear their values
                    if "filter_brokerage" in st.session_state:
                        del st.session_state["filter_brokerage"]
                    if "filter_team" in st.session_state:
                        del st.session_state["filter_team"]
                    if "filter_sales12" in st.session_state:
                        del st.session_state["filter_sales12"]
                    st.session_state.teams_filters_applied = False
                    st.session_state.teams_offset = 0
                    st.session_state.filtered_teams_data = pd.DataFrame()
                    st.rerun()

        # --- Apply Filters Logic for Teams ---
        # Automatically apply filters on first load if not already applied
        if apply_filters_btn or not st.session_state.teams_filters_applied:
            st.session_state.teams_offset = 0
            st.session_state.teams_filters_applied = True
            print("\n--- Applying Team Filters ---")
            with st.spinner("Calculating total teams..."):
                st.session_state.total_teams = get_total_team_count(st.session_state.selected_states_teams)
            if st.session_state.total_teams > 0:
                with st.spinner("Loading team data..."):
                    st.session_state.filtered_teams_data = load_team_data(CACHE_LIMIT_TEAMS, 0,
                                                                          st.session_state.selected_states_teams)
            else:
                st.session_state.filtered_teams_data = pd.DataFrame()
            print("-----------------------------\n")
            if apply_filters_btn:
                st.rerun()

        # --- Display Team Data ---
        current_team_data = st.session_state.get('filtered_teams_data', pd.DataFrame())

        if not current_team_data.empty:
            df_teams_display = current_team_data.copy()
            print(f"Columns in df_teams_display: {df_teams_display.columns.tolist()}")

            # --- Format Columns for Display ---
            if DF_COL_TOTAL_SALES in df_teams_display.columns:
                df_teams_display[DISPLAY_COL_TOTAL_SALES] = df_teams_display[DF_COL_TOTAL_SALES].apply(
                    lambda x: f"{x:,.0f}" if pd.notna(x) else ""
                )
            if DF_COL_SALES_LASTYEAR_TEAMS in df_teams_display.columns:
                df_teams_display[DISPLAY_COL_SALES_LASTYEAR_TEAMS] = df_teams_display[
                    DF_COL_SALES_LASTYEAR_TEAMS].apply(
                    lambda x: f"{x:,.0f}" if pd.notna(x) else ""
                )
            if DF_COL_AVG_SALE_TEAMS in df_teams_display.columns:
                df_teams_display[DISPLAY_COL_AVG_SALE_TEAMS] = df_teams_display[DF_COL_AVG_SALE_TEAMS].apply(
                    lambda x: f"${x:,.0f}" if pd.notna(x) else ""
                )

            # --- Define Columns for Display ---
            columns_to_display_teams = [
                DISPLAY_COL_TEAM_NAME,
                DISPLAY_COL_TEAM_LEAD,
                DISPLAY_COL_TEAM_MEMBERS_COUNT,
                DISPLAY_COL_BROKERAGE,
                DISPLAY_COL_STATE_TEAMS,
                DISPLAY_COL_TOTAL_SALES,
                DISPLAY_COL_SALES_LASTYEAR_TEAMS,
                DISPLAY_COL_AVG_SALE_TEAMS,
                DISPLAY_COL_MEMBERS_LIST
            ]
            valid_columns_to_display_teams = [col for col in columns_to_display_teams if
                                              col in df_teams_display.columns]

            if not valid_columns_to_display_teams:
                st.error("No valid team columns available to display.")
            else:
                # --- Display DataFrame ---
                st.dataframe(
                    df_teams_display[valid_columns_to_display_teams],
                    use_container_width=True,
                    hide_index=True
                )

            # --- Metrics and Download for Teams ---
            col_metric_teams, col_dl_teams = st.columns([2, 1])
            with col_metric_teams:
                st.metric("Teams Displayed", len(df_teams_display))
                st.metric("Total Teams Matching Filters", st.session_state.total_teams)
                start_row_teams = st.session_state.teams_offset + 1
                end_row_teams = st.session_state.teams_offset + len(df_teams_display)
                st.caption(f"Showing teams {start_row_teams}-{end_row_teams} of {st.session_state.total_teams}")
            with col_dl_teams:
                csv_data_teams = df_teams_display[valid_columns_to_display_teams].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Export Displayed Teams as CSV",
                    data=csv_data_teams,
                    file_name="filtered_teams_view.csv",
                    mime="text/csv",
                    key='download_teams_csv'
                )

            # --- Pagination for Teams ---
            if end_row_teams < st.session_state.total_teams:
                if st.button("Load More", key="load_more_teams"):
                    print("\n--- Loading More Teams ---")
                    with st.spinner("Loading more team data..."):
                        new_team_data = load_team_data(
                            CACHE_LIMIT_TEAMS,
                            st.session_state.teams_offset + CACHE_LIMIT_TEAMS,
                            st.session_state.selected_states_teams
                        )
                    if not new_team_data.empty:
                        st.session_state.filtered_teams_data = pd.concat(
                            [st.session_state.filtered_teams_data, new_team_data], ignore_index=True
                        )
                        st.session_state.teams_offset += CACHE_LIMIT_TEAMS
                        print("---------------------------\n")
                        st.rerun()
                    else:
                        st.warning("No more team data found.")
                        print("Load More teams returned empty DataFrame unexpectedly.")
            elif st.session_state.total_teams > 0 and not current_team_data.empty:
                st.success("All matching team data loaded.")

        # --- Handle No Data Scenarios for Teams ---
        elif st.session_state.total_teams == 0 and st.session_state.get('teams_filters_applied', False):
            st.info("No teams match the current filters.")
        else:
            st.info("Apply filters using the sidebar to load team data.")
    else:
        st.info("Please log in to view team data.")