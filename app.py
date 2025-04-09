import streamlit as st

st.set_page_config(page_title="Explorer", layout="wide")

from auth import login, logout, is_authenticated
from views.z_agents import z_agents_view
from views.agents import agents_view
from views.transactions import transactions_view

# Initialize session state keys used in transactions_view
if 'transactions_offset' not in st.session_state:
    st.session_state['transactions_offset'] = 0
if 'filtered_transactions_data' not in st.session_state:
    st.session_state['filtered_transactions_data'] = pd.DataFrame()
if 'selected_states' not in st.session_state:
    st.session_state['selected_states'] = []
if 'total_matching_rows' not in st.session_state:
    st.session_state['total_matching_rows'] = 0

# Initialize session state keys for navigation
session_defaults = {
    "username": "",
    "password": "",
    "authenticated": False,
    "selected_table": "Teams"
}
for key, default in session_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default


def render_sidebar():
    cols = st.columns([1, 3])
    with cols[0]:
        st.image("assets/logo.png", width=60)
    with cols[1]:
        if is_authenticated() and st.button("Logout", key="logout_button", use_container_width=True):
            logout()

    st.markdown("# Data Explorer")

    # If not authenticated, show the login form on the sidebar
    if not is_authenticated():
        login()
    else:
        # Display navigation options when logged in
        options = ["Teams", "Agents", "Transactions"]
        st.session_state.selected_table = st.selectbox("Table", options=options, index=0) # Set index to 0 for "Teams"
        st.markdown("---") # Spacer before filters


def render_main():
    # Render the appropriate view based on the selected table if logged in.
    if is_authenticated():
        selected_table = st.session_state.selected_table
        if selected_table == "Teams":
            z_agents_view()
        elif selected_table == "Agents":
            agents_view()
        elif selected_table == "Transactions":
            transactions_view()
    else:
        # If not authenticated, instruct the user to use the sidebar login
        st.info("Please log in using the sidebar.")


with st.sidebar:
    render_sidebar()

render_main()