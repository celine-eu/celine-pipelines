"""
PV Estimation Dashboard — Trentino rooftop solar potential.

Two views:
- Province: aggregate potential, 5-year plan, financial distributions
- REC Substations: per-cabina primaria analysis for REC investment planning

Usage:
    cd apps/pv_estimation
    streamlit run tools/dashboard.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).parent.parent / "flows"))
from db import get_engine


st.set_page_config(
    page_title="PV Estimation — Trentino",
    page_icon="☀",
    layout="wide",
)

# ---------------------------------------------------------------------------
# DB connection & data loaders
# ---------------------------------------------------------------------------


@st.cache_resource
def _engine():
    return get_engine(
        host=st.session_state.get("db_host", "localhost"),
        port=st.session_state.get("db_port", "15432"),
        user=st.session_state.get("db_user", "postgres"),
        password=st.session_state.get("db_password", "securepassword123"),
        dbname=st.session_state.get("db_name", "datasets"),
    )


def _query(sql):
    with _engine().connect() as conn:
        return pd.read_sql(sa.text(sql), conn)


@st.cache_data(ttl=300)
def load_summary():
    return _query("SELECT * FROM ds_dev_gold.pv_installation_plan_summary ORDER BY install_year")


@st.cache_data(ttl=300)
def load_opportunities():
    return _query("""
    SELECT building_id, user_type, kwp, capex,
        annual_production_kwh, annual_consumption_kwh,
        npv, irr, payback_simple, tasso_autoconsumo, footprint_area_m2
    FROM ds_dev_gold.pv_rooftop_opportunities
    """)


@st.cache_data(ttl=300)
def load_plan_buildings():
    return _query("""
    SELECT p.building_id, o.user_type, p.kwp, p.capex,
        p.annual_production_kwh, p.annual_consumption_kwh,
        p.annual_self_consumed_kwh, p.annual_grid_export_kwh,
        p.npv, p.irr, p.payback_simple, p.tasso_autoconsumo,
        p.install_year, p.footprint_area_m2, p.rank
    FROM ds_dev_gold.pv_installation_plan p
    JOIN ds_dev_gold.pv_rooftop_opportunities o ON o.building_id = p.building_id
    ORDER BY p.install_year, p.rank
    """)


@st.cache_data(ttl=300)
def load_totals():
    return _query("""
    SELECT count(*) as total_buildings,
        round(sum(kwp)::numeric / 1000, 1) as total_mwp,
        round(sum(annual_production_kwh)::numeric / 1000000, 1) as total_gwh,
        round(sum(capex)::numeric / 1000000, 1) as total_investment_meur,
        round(avg(irr)::numeric, 4) as avg_irr,
        round(avg(payback_simple)::numeric, 1) as avg_payback,
        round(avg(npv)::numeric, 0) as avg_npv,
        round(sum(npv)::numeric / 1000000, 1) as total_npv_meur,
        round(avg(tasso_autoconsumo)::numeric, 4) as avg_autoconsumo
    FROM ds_dev_gold.pv_rooftop_opportunities
    """).iloc[0]


@st.cache_data(ttl=300)
def load_type_breakdown():
    return _query("""
    SELECT user_type, count(*) as buildings,
        round(sum(kwp)::numeric / 1000, 1) as mwp,
        round(sum(annual_production_kwh)::numeric / 1000000, 1) as gwh,
        round(sum(capex)::numeric / 1000000, 1) as investment_meur,
        round(sum(npv)::numeric / 1000000, 1) as total_npv_meur,
        round(avg(irr)::numeric, 4) as avg_irr,
        round(avg(payback_simple)::numeric, 1) as avg_payback,
        round(avg(tasso_autoconsumo)::numeric, 4) as avg_autoconsumo
    FROM ds_dev_gold.pv_rooftop_opportunities
    GROUP BY user_type ORDER BY buildings DESC
    """)


@st.cache_data(ttl=300)
def load_cabina_summary():
    return _query("SELECT * FROM ds_dev_gold.rec_cabina_summary ORDER BY buildings DESC")


@st.cache_data(ttl=300)
def load_cabina_plan():
    return _query("SELECT * FROM ds_dev_gold.rec_cabina_plan ORDER BY cod_ac, install_year")


@st.cache_data(ttl=300)
def load_cabina_opportunities():
    return _query("""
    SELECT cod_ac, rag_soc, building_id, user_type, kwp, capex,
        annual_production_kwh, annual_consumption_kwh,
        npv, irr, payback_simple, tasso_autoconsumo, footprint_area_m2
    FROM ds_dev_gold.rec_cabina_opportunities
    """)


# ---------------------------------------------------------------------------
# Shared components
# ---------------------------------------------------------------------------

def render_plan_charts(summary, title_suffix=""):
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=summary["install_year"], y=summary["new_kwp"] / 1000,
                             name="New (MWp)", marker_color="#4ecdc4"))
        fig.add_trace(go.Scatter(x=summary["install_year"], y=summary["cum_kwp"] / 1000,
                                 name="Cumulative (MWp)", mode="lines+markers",
                                 line=dict(color="#ff6b6b", width=3), yaxis="y2"))
        fig.update_layout(title=f"Installed Capacity{title_suffix}", xaxis_title="Year",
                          yaxis_title="New (MWp)",
                          yaxis2=dict(title="Cumulative (MWp)", overlaying="y", side="right"),
                          legend=dict(orientation="h", y=-0.2), height=400)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=summary["install_year"], y=summary["new_investment_eur"] / 1e6,
                             name="New (M€)", marker_color="#45b7d1"))
        cum_inv = summary["cum_investment_eur"] / 1e6 if "cum_investment_eur" in summary else summary["new_investment_eur"].cumsum() / 1e6
        fig.add_trace(go.Scatter(x=summary["install_year"], y=cum_inv,
                                 name="Cumulative (M€)", mode="lines+markers",
                                 line=dict(color="#ff6b6b", width=3), yaxis="y2"))
        fig.update_layout(title=f"Investment{title_suffix}", xaxis_title="Year",
                          yaxis_title="New (M€)",
                          yaxis2=dict(title="Cumulative (M€)", overlaying="y", side="right"),
                          legend=dict(orientation="h", y=-0.2), height=400)
        st.plotly_chart(fig, use_container_width=True)


def render_financial_distributions(df, key_prefix=""):
    filter_type = st.multiselect(
        "Filter by building type", options=sorted(df["user_type"].unique()),
        default=sorted(df["user_type"].unique()), key=f"{key_prefix}_type_filter",
    )
    filtered = df[df["user_type"].isin(filter_type)]

    c1, c2 = st.columns(2)
    with c1:
        fig = px.histogram(filtered, x="irr", color="user_type", nbins=50,
                           title="IRR Distribution",
                           labels={"irr": "Internal Rate of Return", "user_type": "Type"},
                           color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(xaxis_tickformat=".0%", height=400, barmode="overlay")
        fig.update_traces(opacity=0.7)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.histogram(filtered, x="payback_simple", color="user_type", nbins=50,
                           title="Payback Period Distribution",
                           labels={"payback_simple": "Simple Payback (years)", "user_type": "Type"},
                           color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400, barmode="overlay")
        fig.update_traces(opacity=0.7)
        st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig = px.histogram(filtered, x="npv", color="user_type", nbins=50,
                           title="NPV Distribution",
                           labels={"npv": "Net Present Value (€)", "user_type": "Type"},
                           color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400, barmode="overlay")
        fig.update_traces(opacity=0.7)
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        fig = px.histogram(filtered, x="tasso_autoconsumo", color="user_type", nbins=50,
                           title="Self-Consumption Rate",
                           labels={"tasso_autoconsumo": "Self-Consumption Rate", "user_type": "Type"},
                           color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(xaxis_tickformat=".0%", height=400, barmode="overlay")
        fig.update_traces(opacity=0.7)
        st.plotly_chart(fig, use_container_width=True)


def render_energy_balance(plan_df):
    energy = plan_df.groupby("install_year").agg(
        self_consumed=("annual_self_consumed_kwh", lambda x: x.sum() / 1e6),
        grid_export=("annual_grid_export_kwh", lambda x: x.sum() / 1e6),
    ).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=energy["install_year"], y=energy["self_consumed"],
                         name="Self-consumed", marker_color="#4ecdc4"))
    fig.add_trace(go.Bar(x=energy["install_year"], y=energy["grid_export"],
                         name="Grid export", marker_color="#45b7d1"))
    fig.update_layout(title="Energy Balance per Yearly Cohort (GWh)",
                      xaxis_title="Installation year", yaxis_title="GWh/year",
                      barmode="stack", height=400,
                      legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Assumptions")
    st.markdown("""
    **PV sizing**
    - Panel density: 0.130 kWp/m2 (65% usable roof)
    - Residential: sized to consumption + 20%
    - Non-residential cap: 20 kWp (IRPEF threshold)
    - Minimum: 3 kWp

    **Consumption**
    - Residential: 3,500 kWh/year (fixed)
    - Commercial: 100 kWh/m2/year
    - Industrial: 150 kWh/m2/year

    **Financial**
    - CAPEX: 1,500 €/kWp
    - Specific yield: 1,100 kWh/kWp/year
    - Regime: RID (feed-in tariff)
    - Discount rate: 5.5%
    - Useful life: 25 years

    **Classification**
    - < 200 m2, ≤ 3 floors: residential
    - ≥ 500 m2, 1 floor: industrial
    - Other large: commercial
    """)
    st.divider()
    st.caption("Data: Overture Maps, TrentinoPvGis, celine-roi")


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

st.title("Trentino Rooftop Solar Potential")

try:
    totals = load_totals()
except Exception as e:
    st.error(f"Cannot connect to database or gold tables not materialized: {e}")
    st.stop()

tab_province, tab_rec = st.tabs(["Province Overview", "REC Substation Analysis"])

# ===================================================================
# TAB 1 — Province Overview
# ===================================================================

with tab_province:
    st.header("Province-Level Solar Potential")
    st.caption("Aggregate PV installation potential across Provincia di Trento")

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Eligible buildings", f"{totals['total_buildings']:,.0f}")
    k2.metric("Total capacity", f"{totals['total_mwp']:,.1f} MWp")
    k3.metric("Annual production", f"{totals['total_gwh']:,.1f} GWh")
    k4.metric("Total investment", f"{totals['total_investment_meur']:,.1f} M€")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Avg IRR", f"{totals['avg_irr']:.1%}")
    k6.metric("Avg payback", f"{totals['avg_payback']:.1f} years")
    k7.metric("Total NPV (25yr)", f"{totals['total_npv_meur']:,.1f} M€")
    k8.metric("Avg self-consumption", f"{totals['avg_autoconsumo']:.1%}")

    st.divider()

    # 5-year plan
    st.subheader("5-Year Installation Plan")
    try:
        summary = load_summary()
        if not summary.empty:
            render_plan_charts(summary)

            display_cols = {
                "install_year": "Year", "new_buildings": "New buildings",
                "cum_buildings": "Cumulative", "coverage_pct": "Coverage %",
                "new_kwp": "New kWp", "cum_mwp": "Cum. MWp",
                "new_production_mwh": "New MWh",
                "cum_annual_production_mwh": "Cum. MWh",
                "new_investment_eur": "New inv. €",
                "cum_investment_meur": "Cum. inv. M€",
                "weighted_avg_irr": "Wtd IRR", "avg_payback_years": "Avg payback",
            }
            st.dataframe(
                summary[list(display_cols.keys())].rename(columns=display_cols).style.format({
                    "New buildings": "{:,.0f}", "Cumulative": "{:,.0f}",
                    "Coverage %": "{:.1f}%", "New kWp": "{:,.0f}",
                    "Cum. MWp": "{:.2f}", "New MWh": "{:,.0f}",
                    "Cum. MWh": "{:,.0f}", "New inv. €": "{:,.0f}",
                    "Cum. inv. M€": "{:.2f}", "Wtd IRR": "{:.1%}",
                    "Avg payback": "{:.1f}",
                }),
                use_container_width=True, hide_index=True,
            )
    except Exception:
        st.info("Installation plan not materialized. Run: dbt seed && dbt run --select gold")

    st.divider()

    # Building type breakdown
    st.subheader("Building Type Breakdown")
    type_df = load_type_breakdown()

    tc1, tc2 = st.columns(2)
    with tc1:
        fig = px.pie(type_df, values="buildings", names="user_type",
                     title="By number of buildings",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    with tc2:
        fig = px.pie(type_df, values="mwp", names="user_type",
                     title="By installed capacity (MWp)",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        type_df.rename(columns={
            "user_type": "Type", "buildings": "Buildings", "mwp": "MWp",
            "gwh": "GWh/yr", "investment_meur": "Inv. M€",
            "total_npv_meur": "Total NPV M€",
            "avg_irr": "Avg IRR", "avg_payback": "Payback",
            "avg_autoconsumo": "Self-cons.",
        }).style.format({
            "Buildings": "{:,.0f}", "MWp": "{:,.1f}", "GWh/yr": "{:,.1f}",
            "Inv. M€": "{:,.1f}", "Total NPV M€": "{:,.1f}",
            "Avg IRR": "{:.1%}", "Payback": "{:.1f}", "Self-cons.": "{:.1%}",
        }),
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # Financial distributions
    st.subheader("Financial Analysis")
    opps = load_opportunities()
    render_financial_distributions(opps, key_prefix="province")

    st.divider()

    # Energy balance
    st.subheader("Energy Balance")
    try:
        plan = load_plan_buildings()
        if not plan.empty:
            render_energy_balance(plan)
    except Exception:
        st.info("Installation plan not available")


# ===================================================================
# TAB 2 — REC Substation Analysis
# ===================================================================

with tab_rec:
    st.header("REC Investment Analysis by Primary Substation")
    st.caption(
        "Italian RECs operate under GSE primary substation (cabina primaria) coverage areas. "
        "Select one or more substations to evaluate the community-level PV investment opportunity."
    )

    try:
        cab_summary = load_cabina_summary()
        cab_plan = load_cabina_plan()
    except Exception as e:
        st.error(f"REC tables not available. Run: dbt run --select rec_it — {e}")
        st.stop()

    if cab_summary.empty:
        st.info("No cabina primaria data.")
        st.stop()

    # Substation selector
    cabina_options = cab_summary.apply(
        lambda r: f"{r['cod_ac']} — {r['rag_soc']} ({r['buildings']:,.0f} buildings)", axis=1,
    ).tolist()
    cod_ac_list = cab_summary["cod_ac"].tolist()

    default_codes = ["AC221E00023", "AC221E00024", "AC221E00027"]
    default_selection = [o for o in cabina_options if any(c in o for c in default_codes)]
    if not default_selection:
        default_selection = cabina_options[:3] if len(cabina_options) >= 3 else cabina_options

    selected = st.multiselect(
        "Select primary substations (cabine primarie)",
        options=cabina_options, default=default_selection,
    )

    selected_codes = [cod_ac_list[cabina_options.index(s)] for s in selected]

    if not selected_codes:
        st.info("Select at least one substation.")
        st.stop()

    sel_summary = cab_summary[cab_summary["cod_ac"].isin(selected_codes)]
    sel_plan = cab_plan[cab_plan["cod_ac"].isin(selected_codes)]

    # ----- Aggregated REC KPIs -----
    st.subheader("Community Investment Summary")

    total_buildings = sel_summary["buildings"].sum()
    total_kwp = sel_summary["total_kwp"].sum()
    total_mwp = sel_summary["total_mwp"].sum()
    total_prod = sel_summary["total_production_mwh"].sum()
    total_cons = sel_summary["total_consumption_mwh"].sum()
    total_self = sel_summary["total_self_consumed_mwh"].sum()
    total_export = sel_summary["total_grid_export_mwh"].sum()
    total_inv = sel_summary["total_investment_meur"].sum()
    total_npv = sel_summary["total_npv_eur"].sum()
    w_irr = (sel_summary["weighted_avg_irr"] * sel_summary["total_kwp"]).sum() / total_kwp if total_kwp else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Eligible buildings", f"{total_buildings:,.0f}")
    k2.metric("Total capacity", f"{total_mwp:,.2f} MWp")
    k3.metric("Annual PV production", f"{total_prod:,.0f} MWh/yr")
    k4.metric("Total investment required", f"{total_inv:,.2f} M€")

    k5, k6, k7, k8 = st.columns(4)
    k5.metric("Community NPV (25yr)", f"{total_npv / 1e6:,.2f} M€")
    k6.metric("Weighted IRR", f"{w_irr:.1%}")
    k7.metric("Self-consumed", f"{total_self:,.0f} MWh/yr")
    k8.metric("Grid export (RID revenue)", f"{total_export:,.0f} MWh/yr")

    # ROI breakdown
    st.markdown("---")
    st.subheader("Return on Investment Breakdown")
    st.markdown(
        "For a REC investing in PV installations across the selected substations, "
        "the aggregate community return is:"
    )

    roi_c1, roi_c2, roi_c3 = st.columns(3)

    roi_ratio = total_npv / (total_inv * 1e6) * 100 if total_inv > 0 else 0
    avg_payback = (sel_summary["avg_payback_years"] * sel_summary["buildings"]).sum() / total_buildings if total_buildings else 0

    roi_c1.metric("Investment", f"{total_inv:,.2f} M€",
                  help="Total CAPEX for PV installations across selected substations")
    roi_c2.metric("Net return (NPV)", f"{total_npv / 1e6:,.2f} M€",
                  delta=f"{roi_ratio:+.1f}% of investment",
                  help="Discounted net value over 25 years at 5.5% WACC")
    roi_c3.metric("Avg payback", f"{avg_payback:.1f} years",
                  help="Weighted average time to recover investment")

    # Revenue streams table
    st.markdown("**Annual revenue streams (year 1 estimate)**")
    retail_price = 0.25
    rid_tariff = 0.04

    annual_self_savings = total_self * 1000 * retail_price
    annual_rid_revenue = total_export * 1000 * rid_tariff
    annual_total_revenue = annual_self_savings + annual_rid_revenue

    rev_df = pd.DataFrame([
        {"Stream": "Self-consumption savings", "MWh/yr": f"{total_self:,.0f}",
         "Unit price": f"{retail_price:.2f} €/kWh", "Annual €": f"{annual_self_savings:,.0f}",
         "Share": f"{annual_self_savings / annual_total_revenue:.0%}" if annual_total_revenue else "—"},
        {"Stream": "Grid export (RID)", "MWh/yr": f"{total_export:,.0f}",
         "Unit price": f"{rid_tariff:.2f} €/kWh", "Annual €": f"{annual_rid_revenue:,.0f}",
         "Share": f"{annual_rid_revenue / annual_total_revenue:.0%}" if annual_total_revenue else "—"},
        {"Stream": "Total", "MWh/yr": f"{total_prod:,.0f}",
         "Unit price": "—", "Annual €": f"{annual_total_revenue:,.0f}", "Share": "100%"},
    ])
    st.dataframe(rev_df, use_container_width=True, hide_index=True)

    st.divider()

    # ----- Per-substation table -----
    st.subheader("Per-Substation Detail")

    cab_display = sel_summary[[
        "cod_ac", "rag_soc", "buildings", "total_mwp",
        "total_production_mwh", "total_self_consumed_mwh", "total_grid_export_mwh",
        "total_investment_meur", "total_npv_eur",
        "weighted_avg_irr", "avg_payback_years",
        "residential_buildings", "commercial_buildings", "industrial_buildings",
    ]].copy()
    cab_display["roi_pct"] = (cab_display["total_npv_eur"] / (cab_display["total_investment_meur"] * 1e6) * 100).round(1)

    st.dataframe(
        cab_display.rename(columns={
            "cod_ac": "Code AC", "rag_soc": "Operator", "buildings": "Buildings",
            "total_mwp": "MWp", "total_production_mwh": "Prod. MWh",
            "total_self_consumed_mwh": "Self-cons. MWh",
            "total_grid_export_mwh": "Export MWh",
            "total_investment_meur": "Inv. M€", "total_npv_eur": "NPV €",
            "weighted_avg_irr": "IRR", "avg_payback_years": "Payback",
            "residential_buildings": "Resid.", "commercial_buildings": "Comm.",
            "industrial_buildings": "Indust.", "roi_pct": "ROI %",
        }).style.format({
            "Buildings": "{:,.0f}", "MWp": "{:.2f}",
            "Prod. MWh": "{:,.0f}", "Self-cons. MWh": "{:,.0f}",
            "Export MWh": "{:,.0f}", "Inv. M€": "{:.2f}",
            "NPV €": "{:,.0f}", "IRR": "{:.1%}",
            "Payback": "{:.1f}", "Resid.": "{:,.0f}",
            "Comm.": "{:,.0f}", "Indust.": "{:,.0f}",
            "ROI %": "{:.1f}%",
        }),
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # ----- 5-year plan -----
    if not sel_plan.empty:
        st.subheader("5-Year Installation Plan")

        plan_agg = sel_plan.groupby("install_year").agg(
            new_buildings=("new_buildings", "sum"),
            cum_buildings=("cum_buildings", "sum"),
            new_kwp=("new_kwp", "sum"),
            cum_kwp=("cum_kwp", "sum"),
            new_production_mwh=("new_production_mwh", "sum"),
            new_investment_eur=("new_investment_eur", "sum"),
            cum_investment_eur=("cum_investment_eur", "sum"),
            new_npv_eur=("new_npv_eur", "sum"),
        ).reset_index()

        render_plan_charts(plan_agg, title_suffix=" — Selected Substations")

        # Cumulative ROI over the plan
        plan_agg["cum_npv"] = plan_agg["new_npv_eur"].cumsum()
        plan_agg["cum_roi_pct"] = (plan_agg["cum_npv"] / plan_agg["cum_investment_eur"] * 100).round(1)

        st.markdown("**Yearly plan detail**")
        plan_display = plan_agg.copy()
        plan_display["new_inv_meur"] = plan_display["new_investment_eur"] / 1e6
        plan_display["cum_inv_meur"] = plan_display["cum_investment_eur"] / 1e6
        plan_display["cum_npv_meur"] = plan_display["cum_npv"] / 1e6

        st.dataframe(
            plan_display[["install_year", "new_buildings", "cum_buildings",
                          "new_kwp", "cum_kwp", "new_production_mwh",
                          "new_inv_meur", "cum_inv_meur",
                          "cum_npv_meur", "cum_roi_pct"]].rename(columns={
                "install_year": "Year", "new_buildings": "New",
                "cum_buildings": "Cum. buildings", "new_kwp": "New kWp",
                "cum_kwp": "Cum. kWp", "new_production_mwh": "New MWh",
                "new_inv_meur": "New inv. M€", "cum_inv_meur": "Cum. inv. M€",
                "cum_npv_meur": "Cum. NPV M€", "cum_roi_pct": "Cum. ROI %",
            }).style.format({
                "New": "{:,.0f}", "Cum. buildings": "{:,.0f}",
                "New kWp": "{:,.0f}", "Cum. kWp": "{:,.0f}",
                "New MWh": "{:,.0f}", "New inv. M€": "{:.2f}",
                "Cum. inv. M€": "{:.2f}", "Cum. NPV M€": "{:.2f}",
                "Cum. ROI %": "{:.1f}%",
            }),
            use_container_width=True, hide_index=True,
        )

        # Compare substations
        if len(selected_codes) > 1:
            st.divider()
            st.subheader("Substation Comparison")
            fig = px.bar(sel_plan, x="install_year", y="new_kwp", color="cod_ac",
                         title="New kWp per Year by Substation",
                         labels={"new_kwp": "New kWp", "install_year": "Year", "cod_ac": "Code AC"},
                         barmode="stack", color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ----- Financial distributions for selected substations -----
    st.subheader("Financial Analysis — Selected Substations")
    try:
        cab_opps = load_cabina_opportunities()
        sel_opps = cab_opps[cab_opps["cod_ac"].isin(selected_codes)]
        if not sel_opps.empty:
            render_financial_distributions(sel_opps, key_prefix="rec")
    except Exception:
        st.info("Detailed opportunities not available")
