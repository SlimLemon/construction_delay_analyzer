# app.py
import pandas as pd
import streamlit as st

from api import run_tia_with_windows

st.set_page_config(
    page_title="Construction TIA Schedule Analyzer",
    layout="wide",
)

st.title("Construction TIA Schedule Analyzer")

st.markdown(
    "Upload a **baseline** P6 XER and one or more **update** XERs, "
    "run Time Impact–style window analysis, and export results to Excel "
    "(and optionally PDF)."
)

# --- Sidebar options ---
st.sidebar.header("Analysis Options")

window_mode_label = st.sidebar.radio(
    "Windowing mode",
    [
        "Baseline start → last update finish (monthly windows)",
        "Custom windows (~30-day buckets)",
    ],
)

if window_mode_label.startswith("Baseline"):
    window_mode = "baseline_range"
else:
    window_mode = "updates_range"

generate_pdf = st.sidebar.checkbox("Generate PDF report as well", value=False)

# --- File upload ---
st.subheader("1. Upload schedules")

baseline_file = st.file_uploader(
    "Baseline schedule (.xer)",
    type=["xer"],
    accept_multiple_files=False,
)

update_files = st.file_uploader(
    "Update schedules (.xer) — select one or more",
    type=["xer"],
    accept_multiple_files=True,
)

st.subheader("2. Run analysis")

run_button = st.button("Run TIA / Window Analysis", type="primary")

if run_button:
    if not baseline_file:
        st.error("Please upload a baseline XER file.")
    elif not update_files:
        st.error("Please upload at least one update XER.")
    else:
        with st.spinner("Analyzing schedules..."):
            try:
                result = run_tia_with_windows(
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

        c1, c2, c3 = st.columns(3)
        c1.metric(
            "Baseline Finish",
            str(summary["baseline_finish"]) if summary["baseline_finish"] else "N/A",
        )
        c2.metric(
            "Current Finish",
            str(summary["current_finish"]) if summary["current_finish"] else "N/A",
        )
        c3.metric(
            "Total Delay (days)",
            f'{summary["total_delay_days"]:.1f}',
        )

        c4, c5, c6 = st.columns(3)
        c4.metric(
            "SPI",
            f'{summary["spi"]:.2f}' if summary["spi"] is not None else "N/A",
        )
        c5.metric(
            "Owner Delay (days)",
            f'{summary["owner_delay_days"]:.1f}',
        )
        c6.metric(
            "Contractor Delay (days)",
            f'{summary["contractor_delay_days"]:.1f}',
        )

        c7, c8, c9 = st.columns(3)
        c7.metric(
            "Concurrent Delay (days)",
            f'{summary["concurrent_delay_days"]:.1f}',
        )
        c8.metric(
            "# Windows",
            summary["num_windows"],
        )
        c9.metric(
            "# Critical Path Shifts",
            summary["cp_shift_count"],
        )

        c10, c11, c12 = st.columns(3)
        c10.metric("Windows w/ Owner Delay", summary["windows_with_owner_delay"])
        c11.metric(
            "Windows w/ Contractor Delay",
            summary["windows_with_contractor_delay"],
        )
        c12.metric(
            "Windows w/ Concurrent Delay",
            summary["windows_with_concurrent_delay"],
        )

        # --- Window table ---
        if window_summaries:
            st.subheader("4. Forensic Windows")
            df = pd.DataFrame(window_summaries)
            st.dataframe(df, use_container_width=True)

        # --- Downloads ---
        st.subheader("5. Download reports")

        with open(result["excel_path"], "rb") as f:
            st.download_button(
                label="Download Excel Workbook",
                data=f,
                file_name="schedule_analysis.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )

        if generate_pdf and result["pdf_path"]:
            with open(result["pdf_path"], "rb") as f:
                st.download_button(
                    label="Download PDF Report",
                    data=f,
                    file_name="schedule_analysis.pdf",
                    mime="application/pdf",
                )