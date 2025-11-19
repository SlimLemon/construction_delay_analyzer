# api.py
from __future__ import annotations

import os
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Literal

from analysis_engine import AnalysisEngine
from forensic_windows import ForensicWindowAnalyzer, ForensicWindow
from report_generator import ReportGenerator
from models import ComparisonResult, DelayEvent, Schedule

WindowMode = Literal["baseline_range", "updates_range"]


def _save_uploaded_file(uploaded_file) -> str:
    """
    Persist a Streamlit UploadedFile to a temp file and return its path.
    """
    suffix = os.path.splitext(uploaded_file.name)[1] or ".xer"
    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return temp_path


def _default_config() -> Dict:
    """
    Minimal default config so AnalysisEngine / ForensicWindowAnalyzer / ReportGenerator
    all initialize cleanly.
    """
    return {
        "analysis": {
            "critical_path_threshold": 0,
            "significant_delay_threshold": 5,
        },
        "reports": {
            "company_name": "Construction Delay Analyzer",
        },
    }


def run_tia_with_windows(
    baseline_file,
    update_files: List,
    window_mode: WindowMode,
    generate_pdf: bool = False,
    config: Dict | None = None,
) -> Dict[str, Any]:
    """
    End-to-end orchestration for the Streamlit UI.

    Inputs:
      - baseline_file: Streamlit UploadedFile (.xer)
      - update_files: list[UploadedFile] (.xer)
      - window_mode: "baseline_range" or "updates_range"
      - generate_pdf: whether to also produce a PDF report
      - config: optional config dict for analysis/reporting

    Returns a dict:
      {
        "summary": {...},
        "window_summaries": [...],
        "excel_path": str,
        "pdf_path": Optional[str],
    }
    """
    if not update_files:
        raise ValueError("At least one update schedule is required.")

    if config is None:
        config = _default_config()

    # --- 1. Initialize core components ---
    engine = AnalysisEngine(config=config)
    fw_analyzer = ForensicWindowAnalyzer(config=config)
    report_gen = ReportGenerator(config=config)

    # --- 2. Save uploaded files to temp paths ---
    baseline_path = _save_uploaded_file(baseline_file)
    update_paths = [_save_uploaded_file(f) for f in update_files]

    # --- 3. Parse baseline and LAST update into engine for global comparison ---
    baseline_schedule: Schedule = engine.parse_xer_file(
        baseline_path,
        schedule_type="baseline",
    )
    current_schedule: Schedule = engine.parse_xer_file(
        update_paths[-1],
        schedule_type="current",
    )

    # --- 4. Compare schedules -> ComparisonResult ---
    comparison: ComparisonResult = engine.compare_schedules()

    # --- 5. Identify delay events & concurrent delays (for responsibility stats) ---
    delay_events: List[DelayEvent] = engine.identify_delay_events(comparison)
    _ = engine.identify_concurrent_delays(delay_events)  # marks .is_concurrent in-place

    # --- 6. Build forensic windows across ALL updates ---
    #    Need each update as a Schedule, not just the last one
    update_schedules: List[Schedule] = []
    for path in update_paths:
        sched = engine.parse_xer_file(path, schedule_type="current")
        update_schedules.append(sched)

    # Decide the date range for windows
    start_date = baseline_schedule.start_date
    # end_date = last update finish
    end_date = update_schedules[-1].finish_date

    if window_mode == "baseline_range":
        # Monthly windows from baseline start to last update finish
        windows: List[ForensicWindow] = fw_analyzer.create_monthly_windows(
            start_date=start_date,
            end_date=end_date,
        )
    elif window_mode == "updates_range":
        # Custom windows bounded by update date range (roughly one window per month/period)
        # Here we use a generic period; you can refine later if you want
        period_days = 30
        windows: List[ForensicWindow] = fw_analyzer.create_custom_windows(
            start_date=start_date,
            end_date=end_date,
            period_days=period_days,
        )
    else:
        raise ValueError(f"Unsupported window_mode: {window_mode}")

    # Analyze each window using baseline vs final current for now.
    # Later you can refine to before/after per-update if needed.
    analyzed_windows: List[ForensicWindow] = []
    for w in windows:
        analyzed = fw_analyzer.analyze_window(
            window=w,
            baseline_schedule=baseline_schedule,
            current_schedule=current_schedule,
        )
        analyzed_windows.append(analyzed)

    # Compute concurrent delay groups per window (optional, but you already have it)
    _ = fw_analyzer.analyze_concurrent_delays(analyzed_windows)

    # --- 7. Export to Excel/PDF using ReportGenerator ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_path = os.path.join(
        tempfile.gettempdir(),
        f"schedule_analysis_{timestamp}.xlsx",
    )
    pdf_path = None
    if generate_pdf:
        pdf_path = os.path.join(
            tempfile.gettempdir(),
            f"schedule_analysis_{timestamp}.pdf",
        )

    # This gives you the full Excel with summary, delays, float, milestones, windows
    report_gen.export_to_excel(
        comparison=comparison,
        windows=analyzed_windows,
        output_path=excel_path,
    )
    if generate_pdf and pdf_path is not None:
        report_gen.generate_pdf_report(
            comparison=comparison,
            windows=analyzed_windows,
            output_path=pdf_path,
        )

    # --- 8. Summaries for the UI ---

    # Responsibility buckets from delay_events (DelayEvent has responsible_party & is_concurrent)
    owner_delay_days = 0.0
    contractor_delay_days = 0.0
    concurrent_delay_days = 0.0

    for d in delay_events:
        if d.is_concurrent:
            concurrent_delay_days += d.delay_days
        else:
            rp = (d.responsible_party or "").upper()
            if rp == "OWNER":
                owner_delay_days += d.delay_days
            elif rp == "CONTRACTOR":
                contractor_delay_days += d.delay_days

    # Critical path shifts & window classifications
    cp_shift_count = 0
    windows_with_owner_delay = 0
    windows_with_contractor_delay = 0
    windows_with_concurrent_delay = 0

    window_summaries: List[Dict[str, Any]] = []

    for w in analyzed_windows:
        # count CP shifts in window
        cp_changed = bool(
            w.critical_path_changes.get("new_critical")
            or w.critical_path_changes.get("removed_critical")
        )
        if cp_changed:
            cp_shift_count += 1

        has_owner = False
        has_contractor = False
        has_concurrent = False

        for de in w.delays:
            if de.is_concurrent:
                has_concurrent = True
            rp = (de.responsible_party or "").upper()
            if rp == "OWNER":
                has_owner = True
            elif rp == "CONTRACTOR":
                has_contractor = True

        if has_owner:
            windows_with_owner_delay += 1
        if has_contractor:
            windows_with_contractor_delay += 1
        if has_concurrent:
            windows_with_concurrent_delay += 1

        window_summaries.append(
            {
                "window_id": w.window_id,
                "start": w.start_date,
                "end": w.end_date,
                "total_delay_days": w.get_total_delay(),
                "num_delays": len(w.delays),
                "num_critical_delays": len(w.get_critical_delays()),
                "cp_changed": cp_changed,
            }
        )

    comparison_summary = {
        "baseline_finish": comparison.baseline_schedule.finish_date,
        "current_finish": comparison.current_schedule.finish_date,
        "total_delay_days": comparison.overall_delay,
        "spi": comparison.spi,
        "completion_variance": comparison.completion_variance,
        "owner_delay_days": owner_delay_days,
        "contractor_delay_days": contractor_delay_days,
        "concurrent_delay_days": concurrent_delay_days,
        "num_windows": len(analyzed_windows),
        "cp_shift_count": cp_shift_count,
        "windows_with_owner_delay": windows_with_owner_delay,
        "windows_with_contractor_delay": windows_with_contractor_delay,
        "windows_with_concurrent_delay": windows_with_concurrent_delay,
    }

    return {
        "summary": comparison_summary,
        "window_summaries": window_summaries,
        "excel_path": excel_path,
        "pdf_path": pdf_path,
    }