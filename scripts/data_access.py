from scripts.data_access import load_str_daily, load_str_monthly
import streamlit as st

df_daily = load_str_daily()
df_monthly = load_str_monthly()

# Example usage
daily_revpar = (
    df_daily[df_daily["metric_name"] == "revpar"]
    .sort_values("asofdate")
)

monthly_revpar = (
    df_monthly[df_monthly["metric_name"] == "revpar"]
    .sort_values("asofdate")
)

st.subheader("Daily RevPAR")
st.line_chart(daily_revpar.set_index("asofdate")["metric_value"])

st.subheader("Monthly RevPAR")
st.line_chart(monthly_revpar.set_index("asofdate")["metric_value"])

