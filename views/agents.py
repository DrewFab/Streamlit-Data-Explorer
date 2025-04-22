import streamlit as st
import pandas as pd
from db import run_query

CACHE_LIMIT_AGENTS = 5000


def load_agents_data(limit=CACHE_LIMIT_AGENTS, offset=0, states=None, agent_name_filter=None, brokerage_filter=None):
    """Loads agent data from the database based on filters."""
    where_clauses = []
    params = {'limit': limit, 'offset': offset}

    if states:
        where_clauses.append('"office_state" IN %(states)s')
        params['states'] = tuple(states)

    if agent_name_filter:
        where_clauses.append('('
                             'LOWER(agent_first_name) LIKE %(name_like)s OR '
                             'LOWER(agent_last_name) LIKE %(name_like)s'
                             ')')
        params['name_like'] = f"%{agent_name_filter}%"

    if brokerage_filter:
        where_clauses.append('LOWER(office_name) LIKE %(brokerage_like)s')
        params['brokerage_like'] = f"%{brokerage_filter}%"

    association_filter = st.session_state.get("filter_association", "").strip().lower()
    if association_filter:
        where_clauses.append('LOWER(association) LIKE %(association_like)s')
        params['association_like'] = f"%{association_filter}%"

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
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
    {where_clause}
    LIMIT %(limit)s OFFSET %(offset)s;
    """

    df = run_query(query, params=params)
    return df


def get_total_agents_count(states=None, agent_name_filter=None, brokerage_filter=None):
    """Counts total number of agents matching the filters."""
    where_clauses = []
    params = {}

    if states:
        where_clauses.append('"office_state" IN %(states)s')
        params['states'] = tuple(states)

    if agent_name_filter:
        where_clauses.append('('
                             'LOWER(agent_first_name) LIKE %(name_like)s OR '
                             'LOWER(agent_last_name) LIKE %(name_like)s'
                             ')')
        params['name_like'] = f"%{agent_name_filter}%"

    if brokerage_filter:
        where_clauses.append('LOWER(office_name) LIKE %(brokerage_like)s')
        params['brokerage_like'] = f"%{brokerage_filter}%"

    association_filter = st.session_state.get("filter_association", "").strip().lower()
    if association_filter:
        where_clauses.append('LOWER(association) LIKE %(association_like)s')
        params['association_like'] = f"%{association_filter}%"

    where_clause = " AND ".join(where_clauses)
    if where_clause:
        where_clause = "WHERE " + where_clause

    query = f"""
    SELECT COUNT(*) FROM agents_master
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
                    st.rerun()
    else:
        if st.session_state.total_agents == 0 and st.session_state.agents_filters_applied:
            st.info("No agents match the current filters.")
        else:
            st.info("Apply filters using the sidebar to load agent data.")
