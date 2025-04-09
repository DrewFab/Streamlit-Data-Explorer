import streamlit as st
from db import run_query
from filters import apply_multiselect_filter
import pandas as pd

def load_data():
    if not st.session_state.authenticated:
        raise ValueError("You must be logged in to view this data.")

    query = """
    SELECT "Name", "Team", "Team_role", "Org", "Street", "City", "State", "Zip", "Office",
           "Phone", "Cell", "Email", "Website", "Facebook", "Linkedin",
           "sales_lastyear"::bigint AS "Sales # (12 Mo.) Raw",
           (CAST("sales_lastyear" AS numeric) * CAST("averageValueThreeYear" AS numeric))::bigint AS "Sales $ (12 Mo.) Raw"
    FROM "z_agents"
    WHERE "Team" IS NOT NULL AND "Team" <> ''
    LIMIT 1000;
    """
    return run_query(query)

def z_agents_view():
    st.title("Teams View")
    if st.session_state.authenticated:
        try:
            df = load_data()

            # Sidebar filters for State and Team Role
            st.sidebar.header("Filter by Location and Role")

            us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                         'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                         'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                         'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                         'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
            selected_states = st.sidebar.multiselect("State", us_states)
            if selected_states:
                df = df[df["State"].isin(selected_states)]

            unique_team_roles = sorted(df["Team_role"].dropna().unique())
            selected_team_roles = st.sidebar.multiselect("Team Role", unique_team_roles)
            if selected_team_roles:
                df = df[df["Team_role"].isin(selected_team_roles)]

            st.subheader("Sales Filters")

            # Expander for Sales Number filter
            with st.expander("Filter 'Sales # (12 Mo.)'"):
                num_col1, num_col2, num_col3 = st.columns([1, 2, 2])
                with num_col1:
                    num_filter_type = st.selectbox("Type", ["=", ">=", "<=", ">", "<", "Between"], key="sales_num_type")
                with num_col2:
                    num_value1 = st.number_input("Value 1", value=df["Sales # (12 Mo.) Raw"].min(), key="sales_num_val1")
                num_value2 = None
                if num_filter_type == "Between":
                    with num_col3:
                        num_value2 = st.number_input("Value 2", value=df["Sales # (12 Mo.) Raw"].max(), key="sales_num_val2")

                if num_filter_type == "=":
                    df = df[df["Sales # (12 Mo.) Raw"] == num_value1]
                elif num_filter_type == ">=":
                    df = df[df["Sales # (12 Mo.) Raw"] >= num_value1]
                elif num_filter_type == "<=":
                    df = df[df["Sales # (12 Mo.) Raw"] <= num_value1]
                elif num_filter_type == ">":
                    df = df[df["Sales # (12 Mo.) Raw"] > num_value1]
                elif num_filter_type == "<":
                    df = df[df["Sales # (12 Mo.) Raw"] < num_value1]
                elif num_filter_type == "Between" and num_value2 is not None:
                    df = df[(df["Sales # (12 Mo.) Raw"] >= num_value1) & (df["Sales # (12 Mo.) Raw"] <= num_value2)]

            # Expander for Sales Dollar filter
            with st.expander("Filter 'Sales $ (12 Mo.)'"):
                dollar_col1, dollar_col2, dollar_col3 = st.columns([1, 2, 2])
                with dollar_col1:
                    dollar_filter_type = st.selectbox("Type", ["=", ">=", "<=", ">", "<", "Between"], key="sales_dollar_type")
                with dollar_col2:
                    dollar_value1 = st.number_input("Value 1", value=df["Sales $ (12 Mo.) Raw"].min(), key="sales_dollar_val1")
                dollar_value2 = None
                if dollar_filter_type == "Between":
                    with dollar_col3:
                        dollar_value2 = st.number_input("Value 2", value=df["Sales $ (12 Mo.) Raw"].max(), key="sales_dollar_val2")

                if dollar_filter_type == "=":
                    df = df[df["Sales $ (12 Mo.) Raw"] == dollar_value1]
                elif dollar_filter_type == ">=":
                    df = df[df["Sales $ (12 Mo.) Raw"] >= dollar_value1]
                elif dollar_filter_type == "<=":
                    df = df[df["Sales $ (12 Mo.) Raw"] <= dollar_value1]
                elif dollar_filter_type == ">":
                    df = df[df["Sales $ (12 Mo.) Raw"] > dollar_value1]
                elif dollar_filter_type == "<":
                    df = df[df["Sales $ (12 Mo.) Raw"] < dollar_value1]
                elif dollar_filter_type == "Between" and dollar_value2 is not None:
                    df = df[(df["Sales $ (12 Mo.) Raw"] >= dollar_value1) & (df["Sales $ (12 Mo.) Raw"] <= dollar_value2)]

            # Format for display
            df_display = df.copy()
            df_display["Sales # (12 Mo.)"] = df_display["Sales # (12 Mo.) Raw"].apply(lambda x: f"{x:,}")
            df_display["Sales $ (12 Mo.)"] = df_display["Sales $ (12 Mo.) Raw"].apply(lambda x: f"${x:,.0f}")

            st.dataframe(df_display[["Name", "Team", "Team_role", "Org", "Street", "City", "State", "Zip", "Office",
                                      "Phone", "Cell", "Email", "Website", "Facebook", "Linkedin",
                                      "Sales # (12 Mo.)", "Sales $ (12 Mo.)"]],
                         use_container_width=True,
                         column_config={
                             "Sales # (12 Mo.)": st.column_config.NumberColumn(
                                 "Sales # (12 Mo.)",
                                 format="%d"
                             ),
                             "Sales $ (12 Mo.)": st.column_config.NumberColumn(
                                 "Sales $ (12 Mo.)",
                                 format="$%d"
                             )
                         })

            col_metric, col_dl = st.columns([1,2])
            with col_metric:
                st.metric("Total Rows", len(df))
            with col_dl:
                st.download_button("Export as CSV", data=df.to_csv(index=False).encode('utf-8'), file_name="z_agents.csv", mime="text/csv")

        except ValueError as e:
            st.error(e)
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    else:
        st.info("Please log in to view Z Agents data.")