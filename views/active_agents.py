import streamlit as st
import pandas as pd
import os
from config import EXPORT_PATH
from db import run_query

CACHE_LIMIT_AGENTS = 5000


def load_agents_data(limit=CACHE_LIMIT_AGENTS, offset=0, states=None, agent_name_filter=None, brokerage_filter=None,
                     state_filter=None, team_filter=None, sales_25_min=None, sales_25_max=None,
                     volume_25_min=None, volume_25_max=None):
    """Loads agent data from the database based on filters."""
    where_clauses = []
    params = {'limit': limit, 'offset': offset}

    # Remove states param, only use state_filter for filtering state

    if agent_name_filter:
        where_clauses.append('('
                             'LOWER(agent_first_name) LIKE %(agent_name_like)s OR '
                             'LOWER(agent_last_name) LIKE %(agent_name_like)s'
                             ')')
        params['agent_name_like'] = f"%{agent_name_filter}%"

    if brokerage_filter:
        where_clauses.append('LOWER(broker) LIKE %(brokerage_like)s')
        params['brokerage_like'] = f"%{brokerage_filter}%"

    if state_filter:
        where_clauses.append('LOWER(office_state) LIKE %(state_like)s')
        params['state_like'] = f"%{state_filter}%"

    if team_filter:
        where_clauses.append('LOWER(team) LIKE %(team_filter)s')
        params['team_filter'] = f"%{team_filter}%"

    if sales_25_min is not None:
        where_clauses.append('sales_25 >= %(sales_25_min)s')
        params['sales_25_min'] = sales_25_min

    if sales_25_max is not None:
        where_clauses.append('sales_25 <= %(sales_25_max)s')
        params['sales_25_max'] = sales_25_max

    if volume_25_min is not None:
        where_clauses.append('volume_25 >= %(volume_25_min)s')
        params['volume_25_min'] = volume_25_min

    if volume_25_max is not None:
        where_clauses.append('volume_25 <= %(volume_25_max)s')
        params['volume_25_max'] = volume_25_max

    association_filter = st.session_state.get("filter_association", "").strip().lower()
    if association_filter:
        where_clauses.append('LOWER(association) LIKE %(association_like)s')
        params['association_like'] = f"%{association_filter}%"

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
    SELECT
      first_name AS "First Name",
      last_name AS "Last Name",
      email AS "Email",
      mobile AS "Mobile",
      broker AS "Broker",
      CONCAT(office_address_1, ' ', COALESCE(office_address_2, '')) AS "Address",
      office_city AS "City",
      office_state AS "State",
      office_zip AS "Zip",
      team AS "Team",
      team_role AS "Role",
      sales_24,
      sales_25,
      volume_24,
      volume_25,
      license_type AS "License Type",
      mlsid AS "MLSID",
      association AS "Association"
    FROM agent_metrics
    {where_clause}
    LIMIT %(limit)s OFFSET %(offset)s;
    """

    df = run_query(query, params=params)
    return df


# New function: same as load_agents_data but without LIMIT/OFFSET
def load_all_agents_data(states=None, agent_name_filter=None, brokerage_filter=None,
                         state_filter=None, team_filter=None, sales_25_min=None, sales_25_max=None,
                         volume_25_min=None, volume_25_max=None):
    """Loads all agent data from the database based on filters (no LIMIT/OFFSET)."""
    where_clauses = []
    params = {}

    # Remove states param, only use state_filter for filtering state

    if agent_name_filter:
        where_clauses.append('('
                             'LOWER(agent_first_name) LIKE %(agent_name_like)s OR '
                             'LOWER(agent_last_name) LIKE %(agent_name_like)s'
                             ')')
        params['agent_name_like'] = f"%{agent_name_filter}%"

    if brokerage_filter:
        where_clauses.append('LOWER(broker) LIKE %(brokerage_like)s')
        params['brokerage_like'] = f"%{brokerage_filter}%"

    if state_filter:
        where_clauses.append('LOWER(office_state) LIKE %(state_like)s')
        params['state_like'] = f"%{state_filter}%"

    if team_filter:
        where_clauses.append('LOWER(team) LIKE %(team_filter)s')
        params['team_filter'] = f"%{team_filter}%"

    if sales_25_min is not None:
        where_clauses.append('sales_25 >= %(sales_25_min)s')
        params['sales_25_min'] = sales_25_min

    if sales_25_max is not None:
        where_clauses.append('sales_25 <= %(sales_25_max)s')
        params['sales_25_max'] = sales_25_max

    if volume_25_min is not None:
        where_clauses.append('volume_25 >= %(volume_25_min)s')
        params['volume_25_min'] = volume_25_min

    if volume_25_max is not None:
        where_clauses.append('volume_25 <= %(volume_25_max)s')
        params['volume_25_max'] = volume_25_max

    association_filter = st.session_state.get("filter_association", "").strip().lower()
    if association_filter:
        where_clauses.append('LOWER(association) LIKE %(association_like)s')
        params['association_like'] = f"%{association_filter}%"

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
    SELECT
      first_name AS "First Name",
      last_name AS "Last Name",
      email AS "Email",
      mobile AS "Mobile",
      broker AS "Broker",
      CONCAT(office_address_1, ' ', COALESCE(office_address_2, '')) AS "Address",
      office_city AS "City",
      office_state AS "State",
      office_zip AS "Zip",
      team AS "Team",
      team_role AS "Role",
      sales_24,
      sales_25,
      volume_24,
      volume_25,
      license_type AS "License Type",
      mlsid AS "MLSID",
      association AS "Association"
    FROM agent_metrics
    {where_clause}
    ;
    """

    df = run_query(query, params=params)
    return df


def get_total_agents_count(states=None, agent_name_filter=None, brokerage_filter=None,
                           state_filter=None, team_filter=None, sales_25_min=None, sales_25_max=None,
                           volume_25_min=None, volume_25_max=None):
    """Counts total number of agents matching the filters."""
    where_clauses = []
    params = {}

    # Remove states param, only use state_filter for filtering state

    if agent_name_filter:
        where_clauses.append('('
                             'LOWER(agent_first_name) LIKE %(agent_name_like)s OR '
                             'LOWER(agent_last_name) LIKE %(agent_name_like)s'
                             ')')
        params['agent_name_like'] = f"%{agent_name_filter}%"

    if brokerage_filter:
        where_clauses.append('LOWER(broker) LIKE %(brokerage_like)s')
        params['brokerage_like'] = f"%{brokerage_filter}%"

    if state_filter:
        where_clauses.append('LOWER(office_state) LIKE %(state_like)s')
        params['state_like'] = f"%{state_filter}%"

    if team_filter:
        where_clauses.append('LOWER(team) LIKE %(team_filter)s')
        params['team_filter'] = f"%{team_filter}%"

    if sales_25_min is not None:
        where_clauses.append('sales_25 >= %(sales_25_min)s')
        params['sales_25_min'] = sales_25_min

    if sales_25_max is not None:
        where_clauses.append('sales_25 <= %(sales_25_max)s')
        params['sales_25_max'] = sales_25_max

    if volume_25_min is not None:
        where_clauses.append('volume_25 >= %(volume_25_min)s')
        params['volume_25_min'] = volume_25_min

    if volume_25_max is not None:
        where_clauses.append('volume_25 <= %(volume_25_max)s')
        params['volume_25_max'] = volume_25_max

    association_filter = st.session_state.get("filter_association", "").strip().lower()
    if association_filter:
        where_clauses.append('LOWER(association) LIKE %(association_like)s')
        params['association_like'] = f"%{association_filter}%"

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
    SELECT COUNT(*) FROM agent_metrics
    {where_clause};
    """

    result = run_query(query, params=params)
    return result.iloc[0][0] if not result.empty else 0


def agents_view():
    st.title("Agents View")

    us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

    # Additional filters
    st.session_state.setdefault('filter_agent', "")
    st.session_state.setdefault('filter_brokerage', "")

    # Session state
    st.session_state.setdefault('agents_offset', 0)
    st.session_state.setdefault('filtered_agents_data', pd.DataFrame())
    st.session_state.setdefault('total_agents', 0)
    st.session_state.setdefault('selected_states_agents', us_states)
    st.session_state.setdefault('agents_filters_applied', False)
    st.session_state.setdefault('load_more_requested', False)

    with st.sidebar:
        st.header("Filter Agents by Location")
        st.text_input("Agent Name", key="filter_agent")
        st.text_input("Brokerage", key="filter_brokerage")
        st.text_input("Association", key="filter_association")

        st.header("Filter Agents by Location")
        select_all_states_agents = st.checkbox("Select All States", key="select_all_states_agents", value=True)
        if select_all_states_agents:
            st.session_state.selected_states_agents = us_states
        else:
            st.session_state.selected_states_agents = st.multiselect(
                "State", us_states, default=st.session_state.selected_states_agents, key="state_select_agents"
            )

        col1, col2 = st.columns(2)
        with col1:
            apply_filters_btn = st.button("Apply Filters", key="apply_filters_agents")
        with col2:
            if st.button("Clear Filters", key="clear_filters_agents"):
                st.session_state.selected_states_agents = us_states
                st.session_state.agents_filters_applied = False
                st.session_state.agents_offset = 0
                st.session_state.filtered_agents_data = pd.DataFrame()
                if "filter_association" in st.session_state:
                    del st.session_state["filter_association"]
                st.rerun()

    agent_name_filter = st.session_state.get("filter_agent", "").strip().lower()
    brokerage_filter = st.session_state.get("filter_brokerage", "").strip().lower()

    if apply_filters_btn or not st.session_state.agents_filters_applied:
        st.session_state.agents_offset = 0
        st.session_state.agents_filters_applied = True
        with st.spinner("Calculating total agents..."):
            st.session_state.total_agents = get_total_agents_count(
                st.session_state.selected_states_agents,
                agent_name_filter,
                brokerage_filter
            )
        if st.session_state.total_agents > 0:
            with st.spinner("Loading agent data..."):
                st.session_state.filtered_agents_data = load_agents_data(
                    CACHE_LIMIT_AGENTS, 0, st.session_state.selected_states_agents, agent_name_filter, brokerage_filter
                )
        else:
            st.session_state.filtered_agents_data = pd.DataFrame()
        if apply_filters_btn:
            st.rerun()

    df_agents = st.session_state.get('filtered_agents_data', pd.DataFrame())

    if not df_agents.empty:
        st.dataframe(df_agents, use_container_width=True, hide_index=True)

        col_metric_agents, col_dl_agents = st.columns([2, 1])
        with col_metric_agents:
            st.metric("Rows Displayed", len(df_agents))
            st.metric("Total Rows Matching Filters", st.session_state.total_agents)
            start = st.session_state.agents_offset + 1
            end = st.session_state.agents_offset + len(df_agents)
            st.caption(f"Showing agents {start}-{end} of {st.session_state.total_agents}")

        with col_dl_agents:
            if len(df_agents) > 15000:
                os.makedirs(EXPORT_PATH, exist_ok=True)
                export_path = os.path.join(EXPORT_PATH, "filtered_agents_view.csv")
                df_agents.to_csv(export_path, index=False)
                with open(export_path, "rb") as f:
                    st.download_button(
                        label="Download Full CSV",
                        data=f,
                        file_name="filtered_agents_view.csv",
                        mime="text/csv",
                        key="download_agents_csv_large"
                    )
            else:
                csv_data = df_agents.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Export Displayed Data as CSV",
                    data=csv_data,
                    file_name="filtered_agents_view.csv",
                    mime="text/csv",
                    key="download_agents_csv"
                )

        if end < st.session_state.total_agents:
            if st.button("Load More", key="load_more_agents"):
                st.session_state.load_more_requested = True

        if st.session_state.load_more_requested:
            with st.spinner("Loading more agent data..."):
                new_agents = load_agents_data(
                    CACHE_LIMIT_AGENTS,
                    st.session_state.agents_offset + CACHE_LIMIT_AGENTS,
                    st.session_state.selected_states_agents,
                    agent_name_filter,
                    brokerage_filter
                )
            if not new_agents.empty:
                st.session_state.filtered_agents_data = pd.concat(
                    [st.session_state.filtered_agents_data, new_agents], ignore_index=True
                )
                st.session_state.agents_offset += CACHE_LIMIT_AGENTS
            st.session_state.load_more_requested = False
            st.rerun()

    else:
        if st.session_state.total_agents == 0 and st.session_state.agents_filters_applied:
            st.info("No agents match the current filters.")
        else:
            st.info("Apply filters using the sidebar to load agent data.")


def active_agents_view():
    st.title("Active Agents")

    # List of US states for dropdown
    us_states = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]

    # Ensure filters exist in session state
    st.session_state.setdefault('filter_agent', "")
    st.session_state.setdefault('filter_brokerage', "")
    st.session_state.setdefault('filter_association', "")
    st.session_state.setdefault('filter_state', "All")
    st.session_state.setdefault('filter_team', "")
    st.session_state.setdefault('sales_25_range', (0, 100000))
    st.session_state.setdefault('volume_25_range', (0.0, 100000000.0))

    with st.sidebar:
        st.selectbox("State", ["All"] + us_states, key="filter_state")
        st.text_input("Broker Filter", key="filter_brokerage")
        st.text_input("Team Filter", key="filter_team")
        st.slider("Sales 2025 Range", min_value=0, max_value=100000, value=(0, 100000), step=100, key="sales_25_range")
        st.slider("Volume 2025 Range", min_value=0.0, max_value=100000000.0, value=(0.0, 100000000.0), step=10000.0, key="volume_25_range")

    # Retrieve filters from session state
    agent_name_filter = st.session_state.get("filter_agent", "").strip().lower()
    brokerage_filter = st.session_state.get("filter_brokerage", "").strip().lower()
    association_filter = st.session_state.get("filter_association", "").strip().lower()
    state_value = st.session_state.get("filter_state", "All")
    # If "All" is selected, do not filter by state; else, use the value
    state_filter = "" if state_value == "All" else state_value.lower()
    team_filter = st.session_state.get("filter_team", "").strip().lower()
    # Unpack slider ranges
    sales_25_min, sales_25_max = st.session_state.get("sales_25_range", (0, 1000))
    volume_25_min, volume_25_max = st.session_state.get("volume_25_range", (0.0, 1000000.0))

    # Load limited data for display
    with st.spinner("Loading active agents data..."):
        df_agents_limited = load_agents_data(
            limit=CACHE_LIMIT_AGENTS,
            offset=0,
            states=None,  # states param removed
            agent_name_filter=agent_name_filter,
            brokerage_filter=brokerage_filter,
            state_filter=state_filter,
            team_filter=team_filter,
            sales_25_min=sales_25_min,
            sales_25_max=sales_25_max,
            volume_25_min=volume_25_min,
            volume_25_max=volume_25_max
        )

        # For full export, use same filter logic as in load_agents_data, but no limit/offset
        df_agents_full = load_all_agents_data(
            states=None,
            agent_name_filter=agent_name_filter,
            brokerage_filter=brokerage_filter,
            state_filter=state_filter,
            team_filter=team_filter,
            sales_25_min=sales_25_min,
            sales_25_max=sales_25_max,
            volume_25_min=volume_25_min,
            volume_25_max=volume_25_max
        )

        total_agents = len(df_agents_full)

    # Format the volume_24 and volume_25 columns for display, if present
    def dollar_fmt(val):
        try:
            return f"${val:,.0f}"
        except Exception:
            return val

    df_agents_limited_display = df_agents_limited.copy()
    for col in ["volume_24", "volume_25"]:
        if col in df_agents_limited_display.columns:
            df_agents_limited_display[col] = df_agents_limited_display[col].apply(dollar_fmt)

    if not df_agents_limited.empty:
        st.dataframe(df_agents_limited_display, use_container_width=True, hide_index=True)

        col_metric_agents, col_dl_agents = st.columns([2, 1])
        with col_metric_agents:
            st.metric("Rows Displayed", len(df_agents_limited))
            st.metric("Total Rows Matching Filters", total_agents)
            st.caption(f"Showing {len(df_agents_limited)} of {total_agents} active agents")

        with col_dl_agents:
            if total_agents > 15000:
                os.makedirs(EXPORT_PATH, exist_ok=True)
                export_path = os.path.join(EXPORT_PATH, "active_agents_view_full.csv")
                df_agents_full.to_csv(export_path, index=False)
                with open(export_path, "rb") as f:
                    st.download_button(
                        label="Download Full CSV",
                        data=f,
                        file_name="active_agents_view_full.csv",
                        mime="text/csv",
                        key="download_active_agents_csv_large"
                    )
            else:
                csv_data = df_agents_full.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Export Full Data as CSV",
                    data=csv_data,
                    file_name="active_agents_view_full.csv",
                    mime="text/csv",
                    key="download_active_agents_csv"
                )
    else:
        st.info("No active agents match the current filters.")
