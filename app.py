import streamlit as st

st.set_page_config(page_title="Explorer", layout="wide")

from auth import login, logout, is_authenticated
from views.z_agents import z_agents_view
from views.agents import agents_view
from views.transactions import transactions_view

# Initialize session state keys if not already set
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
    st.image("assets/logo.png", width=120)
    st.markdown("# Data Explorer")

    # If not authenticated, show the login form on the sidebar
    if not is_authenticated():
        login()
    else:
        # Display navigation options when logged in
        options = ["Teams", "Agents", "Transactions"]
        current_option = st.session_state.selected_table
        index = options.index(current_option) if current_option in options else 0
        st.session_state.selected_table = st.radio("Table", options=options, index=index)
        if st.button("Logout"):
            logout()


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