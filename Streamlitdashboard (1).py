import streamlit as st
import pandas as pd
import pyodbc
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

# ================= PAGE CONFIG =================
st.set_page_config(
    page_title="PLC Central Monitoring",
    page_icon="🏭",
    layout="wide"
)

# ================= STATION MAPPING =================
STATION_INFO = {
    "STATION 04": "CAMSHAFT DROPPING",
    "STATION 07": "BUFFER 1 CP TORQUING",
    "STATION 06": "G.T.H. COVER FITMENT",
    "STATION 05": "REAR COVER FITMENT",
    "STATION 14": "HEAD COVER FITMENT",
    "OFFLINE STATION": "OFFLINE STATION"
}

# ================= IMPROVED CSS =================
st.markdown("""
<style>

/* ===== STATION BUTTONS ===== */
button[kind="primary"] {
    height: 150px !important;
    width: 100% !important;
    min-width: 250px;
    border-radius: 12px;
    border: 2px solid #eab308 !important;
    background-color: #fef9c3 !important;
    color: #854d0e !important;
    font-size: 20px !important;
    font-weight: bold !important;
    white-space: pre-wrap !important;
}

button[kind="primary"]:hover {
    background: linear-gradient(145deg, #facc15, #fbbf24) !important;
    transform: scale(1.03);
}

/* ===== BACK BUTTON ===== */
button[kind="secondary"] {
    height: 55px !important;
    width: 220px !important;
    border-radius: 10px !important;
    background: linear-gradient(145deg, #1e40af, #1d4ed8) !important;
    color: white !important;
    font-size: 16px !important;
    font-weight: 600 !important;
    border: none !important;
}

button[kind="secondary"]:hover {
    background: linear-gradient(145deg, #1d4ed8, #2563eb) !important;
    transform: scale(1.05);
}

/* ===== Animated Arrow ===== */
@keyframes bounceRight {
    0%, 100% { transform: translateX(0); }
    50% { transform: translateX(10px); }
}

.arrow-icon {
    display: inline-block;
    font-size: 28px;
    color: #eab308;
    animation: bounceRight 1.5s infinite;
    margin-right: 12px;
    margin-top: 50px;
}

/* ===== TABLE HEADER ===== */
[data-testid="stDataFrame"] div[role="columnheader"] {
    font-size: 22px !important;
    font-weight: 900 !important;
    color: white !important;
    background-color: #111827 !important;
    text-align: center !important;
}

/* ===== TABLE CELLS ===== */
[data-testid="stDataFrame"] div[role="gridcell"] {
    font-size: 20px !important;
    font-weight: 600 !important;
    padding: 12px !important;
}

/* Alternating row striping */
[data-testid="stDataFrame"] div[role="row"]:nth-child(even) {
    background-color: #f3f4f6 !important;
}

/* Hover effect */
[data-testid="stDataFrame"] div[role="row"]:hover {
    background-color: #e0f2fe !important;
}

/* NG KPI Box */
.ng-box {
    background-color: #fee2e2;
    border: 2px solid #ef4444;
    padding: 18px;
    border-radius: 10px;
    text-align: center;
}

</style>
""", unsafe_allow_html=True)

# ================= SQL CONNECTION =================
def get_conn():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=PLC_Monitoring;"
        "Trusted_Connection=yes;"
    )

@st.cache_data(ttl=5)
def fetch_full_log(station_id):
    try:
        conn = get_conn()
        query = """
            SELECT StationNumber, ModelName, ModelNumber,
                   SetTorque, CycleTime,
                   Status_OK_NG AS Status,
                   FORMAT(Timestamp, 'dd-MMM-yyyy | hh:mm:ss tt') AS [Log Time]
            FROM Production_Log
            WHERE StationNumber = ?
            ORDER BY LogID DESC
        """
        df = pd.read_sql(query, conn, params=[station_id])
        conn.close()
        return df
    except:
        return pd.DataFrame()

# ================= SESSION STATE =================
if "selected_station" not in st.session_state:
    st.session_state.selected_station = None

# ================= MAIN PAGE =================
if st.session_state.selected_station is None:

    st.title("🏭 Central Production Monitoring")

    cols = st.columns(3)

    for i, (s_id, s_name) in enumerate(STATION_INFO.items()):
        with cols[i % 3]:

            st.markdown('<div style="display:flex; align-items:center;">', unsafe_allow_html=True)
            st.markdown('<span class="arrow-icon">➤</span>', unsafe_allow_html=True)

            if st.button(
                f"{s_id}\n{s_name}",
                key=f"btn_{s_id}",
                type="primary"
            ):
                st.session_state.selected_station = s_id
                st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)

# ================= DETAIL PAGE =================
else:

    s_id = st.session_state.selected_station
    s_name = STATION_INFO.get(s_id, "Unknown Station")

    if st.button("⬅ BACK TO MAIN", key="back_btn", type="secondary"):
        st.session_state.selected_station = None
        st.rerun()

    st.title(f"📊 Station: {s_id}")
    st.caption(f"Process: {s_name}")

    log_df = fetch_full_log(s_id)

    if not log_df.empty:

        st.divider()

        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            st.subheader("📈 Production Performance Trends")

        model_list = sorted(log_df["ModelName"].dropna().unique().tolist())
        model_options = ["All Models"] + model_list

        with col2:
            selected_model = st.selectbox("Model", model_options)

        with col3:
            selected_status = st.selectbox("Status", ["All Status", "OK", "NG"])

        model_filtered_df = log_df.copy()

        if selected_model != "All Models":
            model_filtered_df = model_filtered_df[
                model_filtered_df["ModelName"] == selected_model
            ]

        filtered_df = model_filtered_df.copy()

        if selected_status != "All Status":
            filtered_df = filtered_df[
                filtered_df["Status"] == selected_status
            ]

        def style_rows(row):
            if row["Status"] == "OK":
                return ["background-color: #dcfce7; color: #065f46; font-weight: 700;"] * len(row)
            else:
                return ["background-color: #fee2e2; color: #7f1d1d; font-weight: 700;"] * len(row)

        st.dataframe(
            filtered_df.style.apply(style_rows, axis=1),
            hide_index=True,
            use_container_width=True,
            height=520,
            column_config={
                "SetTorque": st.column_config.NumberColumn(
                    "Set Bolt Count",
                    format="%.0f"
                ),
                "CycleTime": st.column_config.NumberColumn(
                    "Cycle Time",
                    format="%.2f sec"
                )
            }
        )

        total_count = len(model_filtered_df)
        ng_count = len(filtered_df[filtered_df["Status"] == "NG"])

        c1, c2, c3 = st.columns(3)

        with c1:
            st.markdown(
                f'<div class="ng-box"><h2 style="color:#b91c1c; margin:0;">{ng_count}</h2>'
                f'<p style="color:#b91c1c; margin:0; font-weight:bold;">NG COUNT</p></div>',
                unsafe_allow_html=True
            )

        with c2:
            st.metric("Total Production", total_count)

        with c3:
            ok_count = len(model_filtered_df[model_filtered_df["Status"] == "OK"])
            ok_rate = round((ok_count / total_count) * 100, 1) if total_count > 0 else 0
            st.metric("Yield Rate", f"{ok_rate}%")

        chart_data = filtered_df.head(50).iloc[::-1]

        c1, c2 = st.columns(2)

        with c1:
            fig_torque = px.line(
                chart_data,
                x="Log Time",
                y="SetTorque",
                color="ModelName" if selected_model == "All Models" else None,
                markers=True
            )
            fig_torque.update_layout(height=300)
            st.plotly_chart(fig_torque, use_container_width=True)

        with c2:
            fig_cycle = px.line(
                chart_data,
                x="Log Time",
                y="CycleTime",
                color="ModelName" if selected_model == "All Models" else None
            )
            fig_cycle.update_layout(height=300)
            st.plotly_chart(fig_cycle, use_container_width=True)

        st.subheader("📊 OK vs NG Distribution")

        status_counts = filtered_df["Status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]

        fig_status = px.bar(
            status_counts,
            x="Status",
            y="Count",
            color="Status",
            color_discrete_map={"OK": "#22c55e", "NG": "#ef4444"}
        )
        fig_status.update_layout(height=350)
        st.plotly_chart(fig_status, use_container_width=True)

    else:
        st.info("Waiting for data from PLC...")

    st_autorefresh(interval=10000, key="refresh")