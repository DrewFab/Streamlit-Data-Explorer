import streamlit as st
import pandas as pd

def render_sidebar_filters(state_options, default_filters=None, view=None):
    """
    Render common sidebar filter controls.

    Parameters:
        state_options (list): List of available states (or any other list for multiselect).
        default_filters (dict): Dictionary containing default filter values.
            For agents view, default keys can be:
              "first_name_filter", "last_name_filter", "filter_brokerage", "city_filter", "state_filter"
            For other views, default keys can be:
              "filter_brokerage", "filter_team", "filter_sales12", "state_filter"
        view (str, optional): If set to 'agents', renders agent-specific filters.

    Returns:
        dict: Dictionary of current filter values.
        bool: True if the Apply Filters button was clicked.
    """
    default_filters = default_filters or {}

    if view == 'agents':
        st.header("Agent Filters")
        first_name_filter = st.text_input(
            "First Name Filter",
            key="first_name_filter",
            value=default_filters.get("first_name_filter", "")
        )
        last_name_filter = st.text_input(
            "Last Name Filter",
            key="last_name_filter",
            value=default_filters.get("last_name_filter", "")
        )
        brokerage_filter = st.text_input(
            "Brokerage Filter",
            key="filter_brokerage",
            value=default_filters.get("filter_brokerage", "")
        )
        city_filter = st.text_input(
            "City Filter",
            key="city_filter",
            value=default_filters.get("city_filter", "")
        )
        state_filter = st.multiselect(
            "State Filter",
            options=state_options,
            key="state_filter",
            default=default_filters.get("state_filter", state_options)
        )
    else:
        st.header("Filters")
        filter_brokerage = st.text_input(
            "Brokerage",
            key="filter_brokerage",
            value=default_filters.get("filter_brokerage", "")
        )
        filter_team = st.text_input(
            "Team",
            key="filter_team",
            value=default_filters.get("filter_team", "")
        )
        filter_sales12 = st.slider(
            "Sales 12 Mo.",
            min_value=0,
            max_value=100,
            value=default_filters.get("filter_sales12", (0, 100)),
            step=1,
            key="filter_sales12"
        )
        state_filter = st.multiselect(
            "State",
            options=state_options,
            key="state_filter",
            default=default_filters.get("state_filter", state_options)
        )

    # Place Apply Filters and Clear Filters buttons next to each other
    cols = st.columns(2)
    apply_clicked = False
    with cols[0]:
        if st.button("Apply Filters"):
            apply_clicked = True
    with cols[1]:
        if st.button("Clear Filters"):
            if view == 'agents':
                for key in ["first_name_filter", "last_name_filter", "filter_brokerage", "city_filter", "state_filter"]:
                    if key in st.session_state:
                        del st.session_state[key]
            else:
                for key in ["filter_brokerage", "filter_team", "filter_sales12", "state_filter"]:
                    if key in st.session_state:
                        del st.session_state[key]
            st.experimental_rerun()

    if view == 'agents':
        current_filters = {
            "first_name_filter": st.session_state.get("first_name_filter", ""),
            "last_name_filter": st.session_state.get("last_name_filter", ""),
            "filter_brokerage": st.session_state.get("filter_brokerage", ""),
            "city_filter": st.session_state.get("city_filter", ""),
            "state_filter": st.session_state.get("state_filter", state_options)
        }
    else:
        current_filters = {
            "filter_brokerage": st.session_state.get("filter_brokerage", ""),
            "filter_team": st.session_state.get("filter_team", ""),
            "filter_sales12": st.session_state.get("filter_sales12", (0, 100)),
            "state_filter": st.session_state.get("state_filter", state_options)
        }

    return current_filters, apply_clicked

def render_bottom_controls(df_display: pd.DataFrame, total_db_rows: int, load_more_callback=None,
                           export_filename="export.csv"):
    """
    Render bottom controls including metrics, a Load More button, and a CSV export button.

    Parameters:
        df_display (pd.DataFrame): The DataFrame currently displayed after filtering.
        total_db_rows (int): The total number of rows matching filters from the database.
        load_more_callback (callable, optional): A function to call when the "Load More" button is pressed.
        export_filename (str): The filename for the export CSV.
    """
    st.metric("Rows Displayed", value=len(df_display))
    st.metric("Total Rows Matching Filters", value=total_db_rows)

    # Always show the Load More button; disable it if no callback is provided
    load_more_disabled = load_more_callback is None
    if st.button("Load More", disabled=load_more_disabled):
        if load_more_callback:
            load_more_callback()
            st.experimental_rerun()

    # Provide a CSV export of the currently displayed DataFrame
    csv = df_display.to_csv(index=False).encode('utf-8')
    st.download_button("Export as CSV", data=csv, file_name=export_filename, mime="text/csv")