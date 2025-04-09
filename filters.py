import streamlit as st
import pandas as pd

def apply_text_filter(df, column, label):
    value = st.text_input(label)
    if value:
        df = df[df[column].str.contains(value, case=False, na=False)]
    return df

def apply_multiselect_filter(df, column, label):
    options = df[column].dropna().unique().tolist()
    selection = st.multiselect(label, options)
    if selection:
        df = df[df[column].isin(selection)]
    return df

def apply_slider_filter(df, column, label):
    if df[column].dtype != 'int64':
        df[column] = pd.to_numeric(df[column], errors='coerce')
    min_val, max_val = int(df[column].min()), int(df[column].max())
    slider = st.slider(label, min_value=min_val, max_value=max_val, value=(min_val, max_val))
    df = df[df[column].between(*slider)]
    return df
