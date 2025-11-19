# app.py
import streamlit as st  # pyright: ignore[reportMissingImports]
from api import run_tia_analysis

st.set_page_config(
    page_title="Construction TIA Schedule Analyzer",
    layout="wide",
)

st.title("Construction TIA Schedule Analyzer")

st.markdown(
    "Upload a **baseline** XER and one or more **update** XERs, "
    "run a Time Impact Analysis (TIA) with automatic windowing, "
    "and export the results to Excel (and optionally PDF)."
)

# --- Sidebar: Options ---
st.sidebar.header("TIA Options")

window_mode_label = st.sidebar.radio(
    "Windowing range",
    [
        "Baseline start to last update finish",
        "Update dates (one window per update interval)",
    ],
)

if "Baseline start to last update finish" in window_mode_label:
    window_mode = "baseline_range"
else:
    window_mode = "updates_range"

generate_pdf = st.sidebar.checkbox("Also generate PDF report", value=False)

# --- Main: File upload section ---
st.subheader("1. Upload schedules")

baseline_file = st.file_uploader(
    "Baseline schedule (.xer)",
    type=["xer"],
    accept_multiple_files=False,
)

update_files = st.file_uploader(
    "Update schedules (.xer) – you can select multiple",
    type=["xer"],
    accept_multiple_files=True,
)

st.subheader("2. Run analysis")

run_button = st.button("Run TIA analysis", type="primary")

if run_button:
    if not baseline_file:
        st.error("Please upload a baseline XER file.")
    elif not update_files:
        st.error("Please upload at least one update XER file.")
    else:
        with st.spinner("Running TIA analysis... this may take a few moments."):
            try:
                result = run_tia_analysis(
                    baseline_file=baseline_file,
                    update_files=update_files,
                    window_mode=window_mode,
                    generate_pdf=generate_pdf,
                )
            except Exception as e:
                st.error(f"Analysis failed: {e}")
                st.stop()

        summary = result["summary"]
        window_summaries = result["window_summaries"]

        st.success("Analysis complete.")

        # --- Summary KPIs ---
        st.subheader("3. Summary")

        col1, col2, col3 = st.columns(3)
        col1.metric(
            "Baseline Finish",
            str(summary["baseline_finish"]) if summary["baseline_finish"] else "N/A",
        )
        col2.metric(
            "Current Finish",
            str(summary["current_finish"]) if summary["current_finish"] else "N/A",
        )
        col3.metric(
            "Total Delay (days)",
            f'{summary["total_delay_days"]:.1f}'
            if summary["total_delay_days"] is not None
            else "N/A",
        )

        col4, col5, col6 = st.columns(3)
        col4.metric(
            "SPI",
            f'{summary["spi"]:.2f}' if summary["spi"] is not None else "N/A",
        )
        col5.metric(
            "Owner Delay (days)",
            f'{summary["owner_delay_days"]:.1f}',
        )
        col6.metric(
            "Contractor Delay (days)",
            f'{summary["contractor_delay_days"]:.1f}',
        )

        col7, col8, col9 = st.columns(3)
        col7.metric(
            "Concurrent Delay (days)",
            f'{summary["concurrent_delay_days"]:.1f}',
        )
        col8.metric(
            "# Forensic Windows",
            summary["num_windows"],
        )
        col9.metric(
            "# Critical Path Shifts",
            summary["cp_shift_count"],
        )

        # Windows breakdown
        st.markdown("**Windows with delay by responsibility**")
        c1, c2, c3 = st.columns(3)
        c1.metric("Windows with Owner Delay", summary["windows_with_owner_delay"])
        c2.metric("Windows with Contractor Delay", summary["windows_with_contractor_delay"])
        c3.metric("Windows with Concurrent Delay", summary["windows_with_concurrent_delay"])

        # --- Optional table of windows ---
        if window_summaries:
            st.subheader("4. Forensic Windows Overview")
            import pandas as pd  # pyright: ignore[reportMissingImports]

            df_windows = pd.DataFrame(window_summaries)
            st.dataframe(df_windows, use_container_width=True)

        # --- Download links ---
        st.subheader("5. Download reports")

        with open(result["excel_path"], "rb") as f:
            st.download_button(
                label="Download Excel workbook",
                data=f,
                file_name="tia_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        if generate_pdf and result["pdf_path"]:
            with open(result["pdf_path"], "rb") as f:
                st.download_button(
                    label="Download PDF report",
                    data=f,
                    file_name="tia_analysis.pdf",
                    mime="application/pdf",
                )