import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
import time

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

# ================= MODEL THRESHOLD MAPPING =================
MODEL_THRESHOLDS = {
    "3.3L-TGH-HC-2.4-24": [2.4, 2.6, 2.2],
    "6BT-CP-HC-2.4-19": [2.4, 2.6, 2.2],
    "ISBE-CP-HC-2.4-20": [2.4, 2.6, 2.2],
    "497-CP-HC-2.4-33": [2.4, 2.6, 2.2],
    "697-CP-HC-2.4-30": [2.4, 2.6, 2.2],
    "1.5L-CONROD-5.0-08": [5.0, 5.2, 4.8],
    "497-TGH-4.5-03": [4.5, 4.7, 4.3],
    "497-TGH-4.5-04": [4.5, 4.7, 4.3],
    "6BT-TGH-CAM-2.4-09": [2.4, 2.6, 2.2],
    "ISBE-TGH-CAM-2.4-08": [2.4, 2.6, 2.2],
    "5L-OPUMP-4.0-05": [4.0, 4.2, 3.8]
}

# ================= DATABASE CONNECTION =================
@st.cache_resource
def get_conn():
    # Cached resource ensures we don't reconnect on every fragment refresh
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=PLC_Monitoring;"
        "Trusted_Connection=yes;"
    )

def fetch_full_log(station_id):
    try:
        conn = get_conn()
        query = """
            SELECT StationNumber, ModelName, ModelNumber,
                   SetTorque AS [Actual Bolt Count], CycleTime,
                   Status_OK_NG AS Status,
                   Timestamp
            FROM Production_Log
            WHERE StationNumber = ?
            ORDER BY LogID DESC
        """
        df = pd.read_sql(query, conn, params=[station_id])
        
        # Format Timestamp for display but keep original for sorting if needed
        df['Log Time'] = df['Timestamp'].dt.strftime('%d-%m-%Y | %I:%M:%S %p')

        def map_values(model):
            return MODEL_THRESHOLDS.get(model, [0.0, 0.0, 0.0])

        if not df.empty:
            mapped_vals = df['ModelName'].apply(map_values)
            df[['SET TORQUE (Nm)', 'USL (Nm)', 'LSL (Nm)']] = pd.DataFrame(mapped_vals.tolist(), index=df.index)
        
        return df
    except Exception as e:
        return pd.DataFrame()

# ================= STYLING =================
def style_rows(row):
    if row["Status"] == "OK":
        return ["background-color: #dcfce7; color: #065f46; font-weight: 700;"] * len(row)
    else:
        return ["background-color: #fee2e2; color: #7f1d1d; font-weight: 700;"] * len(row)

st.markdown("""
<style>
/* Prevent layout jumping */
.stMainContainer { contain: paint; }

button[kind="primary"] {
    height: 150px !important; width: 100% !important; min-width: 250px;
    border-radius: 12px; border: 2px solid #eab308 !important;
    background-color: #fef9c3 !important; color: #854d0e !important;
    font-size: 20px !important; font-weight: bold !important; white-space: pre-wrap !important;
}
button[kind="primary"]:hover { background: linear-gradient(145deg, #facc15, #fbbf24) !important; transform: scale(1.01); }

button[kind="secondary"] {
    height: 55px !important; width: 220px !important; border-radius: 10px !important;
    background: linear-gradient(145deg, #1e40af, #1d4ed8) !important;
    color: white !important; font-size: 16px !important; font-weight: 600 !important;
}

.arrow-icon { display: inline-block; font-size: 28px; color: #eab308; animation: bounceRight 1.5s infinite; margin-right: 12px; margin-top: 50px; }
@keyframes bounceRight { 0%, 100% { transform: translateX(0); } 50% { transform: translateX(10px); } }

[data-testid="stDataFrame"] div[role="columnheader"] {
    font-size: 18px !important; font-weight: 900 !important; color: white !important;
    background-color: #111827 !important; text-align: center !important;
}
.ng-box { background-color: #fee2e2; border: 2px solid #ef4444; padding: 18px; border-radius: 10px; text-align: center; }
</style>
""", unsafe_allow_html=True)

# ================= SESSION STATE =================
if "selected_station" not in st.session_state:
    st.session_state.selected_station = None
if "current_page" not in st.session_state:
    st.session_state.current_page = 1

# ================= MAIN PAGE =================
if st.session_state.selected_station is None:
    st.title("🏭 Central Production Monitoring")
    cols = st.columns(3)
    for i, (s_id, s_name) in enumerate(STATION_INFO.items()):
        with cols[i % 3]:
            st.markdown('<div style="display:flex; align-items:center;">', unsafe_allow_html=True)
            st.markdown('<span class="arrow-icon">➤</span>', unsafe_allow_html=True)
            if st.button(f"{s_id}\n{s_name}", key=f"btn_{s_id}", type="primary"):
                st.session_state.selected_station = s_id
                st.session_state.current_page = 1 
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

    # Fragment allows this section to refresh without the flickering of the whole page
    @st.fragment(run_every=10)
    def live_dashboard():
        log_df = fetch_full_log(s_id)
        
        if not log_df.empty:
            f_col1, f_col2 = st.columns([1, 1])
            with f_col1:
                model_options = ["All Models"] + sorted(log_df["ModelName"].unique().tolist())
                selected_model = st.selectbox("Filter Model", model_options)
            with f_col2:
                selected_status = st.selectbox("Filter Status", ["All Status", "OK", "NG"])

            # Filter Logic
            filtered_df = log_df.copy()
            if selected_model != "All Models":
                filtered_df = filtered_df[filtered_df["ModelName"] == selected_model]
            if selected_status != "All Status":
                filtered_df = filtered_df[filtered_df["Status"] == selected_status]

            # Pagination
            rows_per_page = 15
            total_rows = len(filtered_df)
            total_pages = max(1, (total_rows // rows_per_page) + (1 if total_rows % rows_per_page > 0 else 0))
            
            # Reset page if out of bounds after filtering
            current_p = min(st.session_state.current_page, total_pages)
            start_idx = (current_p - 1) * rows_per_page
            end_idx = start_idx + rows_per_page

            column_order = ["Log Time", "ModelName", "Status", "SET TORQUE (Nm)", "USL (Nm)", "LSL (Nm)", "Actual Bolt Count", "CycleTime"]
            
            st.dataframe(
                filtered_df[column_order].iloc[start_idx:end_idx].style.apply(style_rows, axis=1),
                hide_index=True, use_container_width=True, height=450,
                column_config={
                    "SET TORQUE (Nm)": st.column_config.NumberColumn(format="%.1f"),
                    "USL (Nm)": st.column_config.NumberColumn(format="%.1f"),
                    "LSL (Nm)": st.column_config.NumberColumn(format="%.1f"),
                    "Actual Bolt Count": st.column_config.NumberColumn(format="%.2f"),
                    "CycleTime": st.column_config.NumberColumn("Cycle Time", format="%.2f sec")
                }
            )

            # Pagination Controls (Using columns for local control)
            p1, p2, p3, p4 = st.columns([1, 1, 2, 1])
            with p1:
                if st.button("⏮ First", disabled=(current_p == 1)):
                    st.session_state.current_page = 1
                    st.rerun()
            with p2:
                if st.button("⬅ Prev", disabled=(current_p == 1)):
                    st.session_state.current_page -= 1
                    st.rerun()
            p3.markdown(f"<p style='text-align:center; padding-top:10px;'>Page <b>{current_p}</b> of {total_pages}</p>", unsafe_allow_html=True)
            with p4:
                if st.button("Next ➡", disabled=(current_p == total_pages)):
                    st.session_state.current_page += 1
                    st.rerun()

            # KPIs
            st.divider()
            k1, k2, k3 = st.columns(3)
            ng_count = len(filtered_df[filtered_df["Status"] == "NG"])
            k1.markdown(f'<div class="ng-box"><h2 style="color:#b91c1c; margin:0;">{ng_count}</h2><p style="color:#b91c1c; margin:0; font-weight:bold;">NG COUNT</p></div>', unsafe_allow_html=True)
            k2.metric("Total (Filtered)", total_rows)
            yield_rate = round(((total_rows - ng_count) / total_rows) * 100, 1) if total_rows > 0 else 0
            k3.metric("Yield Rate", f"{yield_rate}%")

            # Graphs Section
            st.subheader("📈 Performance Trends (Last 50 Records)")
            chart_data = filtered_df.head(50).iloc[::-1]
            g1, g2 = st.columns(2)
            
            with g1:
                fig_torque = px.line(chart_data, x="Log Time", y="Actual Bolt Count", 
                                     title="Torque Trend", markers=True,
                                     color="ModelName" if selected_model == "All Models" else None)
                fig_torque.update_layout(margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_torque, use_container_width=True, key="torque_chart")
                
            with g2:
                fig_cycle = px.line(chart_data, x="Log Time", y="CycleTime", 
                                    title="Cycle Time Trend",
                                    color="ModelName" if selected_model == "All Models" else None)
                fig_cycle.update_layout(margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_cycle, use_container_width=True, key="cycle_chart")

            st.subheader("📊 Quality Distribution")
            status_counts = filtered_df["Status"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            fig_status = px.bar(status_counts, x="Status", y="Count", color="Status",
                                color_discrete_map={"OK": "#22c55e", "NG": "#ef4444"})
            st.plotly_chart(fig_status, use_container_width=True, key="status_chart")
        else:
            st.info("Waiting for data from PLC...")

    # Execute the live fragment
    live_dashboard()

