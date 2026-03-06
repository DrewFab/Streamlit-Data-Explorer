import streamlit as st
import pandas as pd
import os
import re
import snowflake.connector
from snowflake.connector import DictCursor
from config import EXPORT_PATH

SNOWFLAKE = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

CACHE_LIMIT = 10_000

CURRENCY_COLS = [
    "Sold Volume", "Avg Sold Price",
    "Pending Volume", "Avg Pending Price",
    "Off Market Volume", "Avg Off Market Price",
    "Total Volume", "Avg Transaction Price",
]
COUNT_COLS = [
    "Sold Count", "Pending Count", "Off Market Count",
    "Total Transaction Count", "Team Members",
]


def _fmt_display(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display copy with currency and count columns formatted as strings."""
    out = df.copy()
    for col in CURRENCY_COLS:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x != "" else ""
            )
    for col in COUNT_COLS:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x != "" else ""
            )
    return out


def connect_snowflake() -> snowflake.connector.SnowflakeConnection:
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None


def run_snowflake_query(query, params=None):
    """Executes a query on Snowflake and returns a DataFrame."""
    sql = query.replace("≥", ">=").replace("≤", "<=")
    if params:
        sql = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"%(\1)s", sql)
    conn = connect_snowflake()
    if conn is None:
        st.error("Snowflake connection was not established.")
        return pd.DataFrame()
    cur = None
    try:
        cur = conn.cursor(DictCursor)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        st.error(f"SQL Query: {sql}")
        if params:
            st.error(f"Parameters: {params}")
        return pd.DataFrame()
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            conn.close()


def _build_where(
    name_filter, broker_filter, email_filter, role_filter, state_filter,
    total_volume_min=None, total_volume_max=None,
    avg_price_min=None, avg_price_max=None,
    txn_count_min=None, txn_count_max=None,
):
    """Shared WHERE clause builder for data and count queries."""
    where_clauses = []
    params = {}

    if name_filter:
        where_clauses.append(
            "(PRESENTED_BY_FIRST_NAME ILIKE :name_like OR PRESENTED_BY_LAST_NAME ILIKE :name_like)"
        )
        params["name_like"] = f"%{name_filter}%"

    if broker_filter:
        where_clauses.append("BROKERED_BY ILIKE :broker_like")
        params["broker_like"] = f"%{broker_filter}%"

    if email_filter:
        where_clauses.append("EMAIL ILIKE :email_like")
        params["email_like"] = f"%{email_filter}%"

    if role_filter:
        where_clauses.append("ROLE ILIKE :role_like")
        params["role_like"] = f"%{role_filter}%"

    if state_filter:
        placeholders = []
        for i, state in enumerate(state_filter):
            key = f"state_{i}"
            placeholders.append(f":{key}")
            params[key] = state
        where_clauses.append(f"STATE IN ({', '.join(placeholders)})")

    if total_volume_min is not None and total_volume_min > 0:
        where_clauses.append("TOTAL_VOLUME >= :total_volume_min")
        params["total_volume_min"] = total_volume_min

    if total_volume_max is not None and total_volume_max < 999_999_999_999:
        where_clauses.append("TOTAL_VOLUME <= :total_volume_max")
        params["total_volume_max"] = total_volume_max

    if avg_price_min is not None and avg_price_min > 0:
        where_clauses.append("AVG_TRANSACTION_PRICE >= :avg_price_min")
        params["avg_price_min"] = avg_price_min

    if avg_price_max is not None and avg_price_max < 999_999_999_999:
        where_clauses.append("AVG_TRANSACTION_PRICE <= :avg_price_max")
        params["avg_price_max"] = avg_price_max

    if txn_count_min is not None and txn_count_min > 0:
        where_clauses.append("TOTAL_TRANSACTION_COUNT >= :txn_count_min")
        params["txn_count_min"] = txn_count_min

    if txn_count_max is not None and txn_count_max < 999_999:
        where_clauses.append("TOTAL_TRANSACTION_COUNT <= :txn_count_max")
        params["txn_count_max"] = txn_count_max

    where_clause = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    return where_clause, params


def get_total_agent_performance_count(
    name_filter=None, broker_filter=None, email_filter=None,
    role_filter=None, state_filter=None,
    total_volume_min=None, total_volume_max=None,
    avg_price_min=None, avg_price_max=None,
    txn_count_min=None, txn_count_max=None,
):
    """Returns the total number of matching rows."""
    where_clause, params = _build_where(
        name_filter, broker_filter, email_filter, role_filter, state_filter,
        total_volume_min, total_volume_max, avg_price_min, avg_price_max,
        txn_count_min, txn_count_max,
    )
    query = f"""
    SELECT COUNT(*) AS "total"
    FROM SCOUT_DW.COMPCURVE.AGENT_PERFORMANCE_WITH_LOCATION_CANON
    {where_clause}
    """
    df = run_snowflake_query(query, params=params or None)
    if not df.empty:
        val = df.iloc[0].get("total", df.iloc[0].get("TOTAL", df.iloc[0].iloc[0]))
        try:
            return int(val if val is not None else 0)
        except Exception:
            return 0
    return 0


def load_agent_performance_data(
    name_filter=None, broker_filter=None, email_filter=None,
    role_filter=None, state_filter=None,
    total_volume_min=None, total_volume_max=None,
    avg_price_min=None, avg_price_max=None,
    txn_count_min=None, txn_count_max=None,
    limit=CACHE_LIMIT, offset=0,
):
    """Loads Agent Performance data from Snowflake with LIMIT/OFFSET."""
    where_clause, params = _build_where(
        name_filter, broker_filter, email_filter, role_filter, state_filter,
        total_volume_min, total_volume_max, avg_price_min, avg_price_max,
        txn_count_min, txn_count_max,
    )
    params["limit"] = limit
    params["offset"] = offset

    query = f"""
    SELECT
        PRESENTED_BY_FIRST_NAME        AS "First Name",
        PRESENTED_BY_LAST_NAME         AS "Last Name",
        BROKERED_BY                    AS "Broker",
        STATE                          AS "State",
        EMAIL                          AS "Email",
        ROLE                           AS "Team Role",
        MEMBER_COUNT                   AS "Team Members",
        BROKERED_BY_HISTORY            AS "Brokered By History",
        SOLD_COUNT                     AS "Sold Count",
        SOLD_VOLUME                    AS "Sold Volume",
        AVG_SOLD_PRICE                 AS "Avg Sold Price",
        PENDING_COUNT                  AS "Pending Count",
        PENDING_VOLUME                 AS "Pending Volume",
        AVG_PENDING_PRICE              AS "Avg Pending Price",
        OFF_MARKET_COUNT               AS "Off Market Count",
        OFF_MARKET_VOLUME              AS "Off Market Volume",
        AVG_OFF_MARKET_PRICE           AS "Avg Off Market Price",
        TOTAL_TRANSACTION_COUNT        AS "Total Transaction Count",
        TOTAL_VOLUME                   AS "Total Volume",
        AVG_TRANSACTION_PRICE          AS "Avg Transaction Price",
        MOST_RECENT_TRANSACTION_DATE   AS "Most Recent Transaction Date",
        OFFICE_ADDRESS_1               AS "Office Address 1",
        OFFICE_ADDRESS_2               AS "Office Address 2",
        OFFICE_CITY                    AS "Office City",
        OFFICE_STATE                   AS "Office State",
        OFFICE_ZIP                     AS "Office Zip"
    FROM SCOUT_DW.COMPCURVE.AGENT_PERFORMANCE_WITH_LOCATION_CANON
    {where_clause}
    ORDER BY PRESENTED_BY_FIRST_NAME ASC NULLS LAST
    LIMIT :limit OFFSET :offset
    """

    return run_snowflake_query(query, params=params)


def agent_performance_view():
    """Main view for Agent Performance data from Snowflake."""
    st.title("Agent Performance")

    # ── Sidebar filters ──────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")
        st.text_input("Name",      key="filter_ap_name",   placeholder="Search by first or last name...")
        st.text_input("Broker",    key="filter_ap_broker", placeholder="Search by broker...")
        st.text_input("Email",     key="filter_ap_email",  placeholder="Search by email...")
        st.text_input("Team Role", key="filter_ap_role",   placeholder="Search by role...")

        state_options = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        ]
        saved = st.session_state.get("filter_ap_state", [])
        st.multiselect(
            "State",
            options=state_options,
            default=[s for s in saved if s in state_options],
            key="filter_ap_state",
        )

        st.markdown("---")

        with st.expander("Total Volume Range", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.number_input("Min ($)", min_value=0, max_value=999_999_999_999,
                                value=0, step=100_000, key="filter_ap_vol_min",
                                format="%d")
            with col2:
                st.number_input("Max ($)", min_value=0, max_value=999_999_999_999,
                                value=999_999_999_999, step=100_000, key="filter_ap_vol_max",
                                format="%d")

        with st.expander("Avg Transaction Price Range", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.number_input("Min ($)", min_value=0, max_value=999_999_999_999,
                                value=0, step=10_000, key="filter_ap_avg_min",
                                format="%d")
            with col2:
                st.number_input("Max ($)", min_value=0, max_value=999_999_999_999,
                                value=999_999_999_999, step=10_000, key="filter_ap_avg_max",
                                format="%d")

        with st.expander("Total Transaction Count Range", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.number_input("Min", min_value=0, max_value=999_999,
                                value=0, step=1, key="filter_ap_txn_min")
            with col2:
                st.number_input("Max", min_value=0, max_value=999_999,
                                value=999_999, step=1, key="filter_ap_txn_max")

        st.markdown("---")

    # ── Session state defaults ───────────────────────────────────────────────
    st.session_state.setdefault("ap_offset", 0)
    st.session_state.setdefault("ap_df", pd.DataFrame())
    st.session_state.setdefault("ap_total", 0)
    st.session_state.setdefault("ap_last_filters", {})

    # ── Collect current filter values ────────────────────────────────────────
    name_filter        = st.session_state.get("filter_ap_name",    "").strip()
    broker_filter      = st.session_state.get("filter_ap_broker",  "").strip()
    email_filter       = st.session_state.get("filter_ap_email",   "").strip()
    role_filter        = st.session_state.get("filter_ap_role",    "").strip()
    state_filter       = st.session_state.get("filter_ap_state",   [])
    total_volume_min   = st.session_state.get("filter_ap_vol_min", 0)
    total_volume_max   = st.session_state.get("filter_ap_vol_max", 999_999_999_999)
    avg_price_min      = st.session_state.get("filter_ap_avg_min", 0)
    avg_price_max      = st.session_state.get("filter_ap_avg_max", 999_999_999_999)
    txn_count_min      = st.session_state.get("filter_ap_txn_min", 0)
    txn_count_max      = st.session_state.get("filter_ap_txn_max", 999_999)

    current_filters = {
        "name_filter":      name_filter,
        "broker_filter":    broker_filter,
        "email_filter":     email_filter,
        "role_filter":      role_filter,
        "state_filter":     tuple(state_filter),
        "total_volume_min": total_volume_min,
        "total_volume_max": total_volume_max,
        "avg_price_min":    avg_price_min,
        "avg_price_max":    avg_price_max,
        "txn_count_min":    txn_count_min,
        "txn_count_max":    txn_count_max,
    }

    filters_changed = current_filters != st.session_state["ap_last_filters"]

    # ── On filter change: reset and reload first page ────────────────────────
    if filters_changed:
        st.session_state["ap_offset"] = 0
        st.session_state["ap_last_filters"] = current_filters.copy()

        with st.spinner("Calculating total records..."):
            st.session_state["ap_total"] = get_total_agent_performance_count(
                name_filter=name_filter or None,
                broker_filter=broker_filter or None,
                email_filter=email_filter or None,
                role_filter=role_filter or None,
                state_filter=state_filter or None,
                total_volume_min=total_volume_min,
                total_volume_max=total_volume_max,
                avg_price_min=avg_price_min,
                avg_price_max=avg_price_max,
                txn_count_min=txn_count_min,
                txn_count_max=txn_count_max,
            )

        if st.session_state["ap_total"] > 0:
            with st.spinner("Loading Agent Performance data..."):
                st.session_state["ap_df"] = load_agent_performance_data(
                    name_filter=name_filter or None,
                    broker_filter=broker_filter or None,
                    email_filter=email_filter or None,
                    role_filter=role_filter or None,
                    state_filter=state_filter or None,
                    total_volume_min=total_volume_min,
                    total_volume_max=total_volume_max,
                    avg_price_min=avg_price_min,
                    avg_price_max=avg_price_max,
                    txn_count_min=txn_count_min,
                    txn_count_max=txn_count_max,
                    limit=CACHE_LIMIT,
                    offset=0,
                )
        else:
            st.session_state["ap_df"] = pd.DataFrame()

        st.rerun()

    # ── Render ───────────────────────────────────────────────────────────────
    df    = st.session_state["ap_df"]
    total = st.session_state["ap_total"]
    offset = st.session_state["ap_offset"]

    if not df.empty:
        st.dataframe(
            _fmt_display(df),
            use_container_width=True,
            hide_index=True,
            height=600,
        )

        col_metric, col_dl = st.columns([2, 1])
        with col_metric:
            st.metric("Rows Displayed", f"{len(df):,}")
            st.metric("Total Rows Matching Filters", f"{total:,}")
            st.caption(f"Showing records 1–{len(df):,} of {total:,}")

        with col_dl:
            csv_data = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Export Displayed Data as CSV",
                data=csv_data,
                file_name="agent_performance_view.csv",
                mime="text/csv",
                key="download_ap_csv",
            )

        # Load More
        if len(df) < total:
            if st.button("Load More", key="load_more_ap"):
                next_offset = offset + CACHE_LIMIT
                with st.spinner("Loading more records..."):
                    more = load_agent_performance_data(
                        name_filter=name_filter or None,
                        broker_filter=broker_filter or None,
                        email_filter=email_filter or None,
                        role_filter=role_filter or None,
                        state_filter=state_filter or None,
                        total_volume_min=total_volume_min,
                        total_volume_max=total_volume_max,
                        avg_price_min=avg_price_min,
                        avg_price_max=avg_price_max,
                        txn_count_min=txn_count_min,
                        txn_count_max=txn_count_max,
                        limit=CACHE_LIMIT,
                        offset=next_offset,
                    )
                if not more.empty:
                    st.session_state["ap_df"] = pd.concat(
                        [st.session_state["ap_df"], more], ignore_index=True
                    )
                    st.session_state["ap_offset"] = next_offset
                st.rerun()
    else:
        if total == 0 and st.session_state["ap_last_filters"]:
            st.info("No Agent Performance records match the current filters.")
        else:
            st.info("Loading data...")
