# api.py
from __future__ import annotations

from dataclasses import asdict
from typing import List, BinaryIO, Literal, Dict, Any

from analysis_engine import AnalysisEngine
from forensic_windows import ForensicWindowAnalyzer
from report_generator import ReportGenerator
from models import ComparisonResult, ForensicWindow, DelayEvent


AnalysisMethod = Literal["TIA"]
WindowMode = Literal["baseline_range", "updates_range"]


def _filelike_to_bytes(file_obj: BinaryIO) -> bytes:
    """Streamlit uploads give you a file-like object; convert to bytes once."""
    file_obj.seek(0)
    return file_obj.read()


def run_tia_analysis(
    baseline_file,
    update_files: List,
    window_mode: WindowMode,
    generate_pdf: bool = False,
) -> Dict[str, Any]:
    """
    High-level orchestration for:
    - 1 baseline XER
    - 1+ update XER
    - TIA-style analysis with automatic windowing
    - Excel output (+ optional PDF)
    
    Returns:
      {
        "summary": {...},
        "window_summaries": [...],
        "excel_path": "path/to.xlsx",
        "pdf_path": "path/to.pdf" or None,
    }
    """

    if not update_files:
        raise ValueError("At least one update schedule is required for TIA analysis.")

    engine = AnalysisEngine()

    # 1) Parse baseline and updates
    baseline_bytes = _filelike_to_bytes(baseline_file)
    update_bytes_list = [_filelike_to_bytes(f) for f in update_files]

    baseline_schedule = engine.parse_schedule_from_bytes(baseline_bytes)
    update_schedules = [
        engine.parse_schedule_from_bytes(b) for b in update_bytes_list
    ]

    # 2) Validate schedules (if your engine exposes validation)
    validation_issues = engine.validate_schedules(
        baseline_schedule,
        update_schedules,
    )

    # 3) Comparison – baseline vs last update for overall snapshot
    comparator = engine.get_comparator()
    comparison_result: ComparisonResult = comparator.compare(
        baseline_schedule,
        update_schedules[-1],
    )

    # 4) Forensic windows – TIA with auto windowing
    fw_analyzer = ForensicWindowAnalyzer()

    if window_mode == "baseline_range":
        # windows from baseline start to last update finish
        windows: List[ForensicWindow] = fw_analyzer.create_tia_windows_from_baseline_range(
            baseline_schedule,
            update_schedules,
        )
    elif window_mode == "updates_range":
        # windows bounded by update dates
        windows: List[ForensicWindow] = fw_analyzer.create_tia_windows_from_updates(
            baseline_schedule,
            update_schedules,
        )
    else:
        raise ValueError(f"Unsupported window_mode: {window_mode}")

    # 5) Run TIA inside each window (if not already done inside create_* methods)
    windows = fw_analyzer.run_tia_on_windows(
        baseline_schedule,
        update_schedules,
        windows,
    )

    # 6) Generate reports (Excel + optional PDF)
    report_gen = ReportGenerator()

    excel_path = report_gen.generate_excel_report(
        comparison_result=comparison_result,
        forensic_windows=windows,
        validation_issues=validation_issues,
    )

    pdf_path = None
    if generate_pdf:
        pdf_path = report_gen.generate_pdf_report(
            comparison_result=comparison_result,
            forensic_windows=windows,
            validation_issues=validation_issues,
        )

    # 7) Build summaries for the UI

    # total delays by responsibility (assuming DelayEvent has these fields)
    owner_delay = 0
    contractor_delay = 0
    concurrent_delay = 0

    for w in windows:
        for de in w.delay_events:  # adjust attribute name if different
            if de.is_concurrent:
                concurrent_delay += de.net_delay_days
            elif de.responsible_party == "OWNER":
                owner_delay += de.net_delay_days
            elif de.responsible_party == "CONTRACTOR":
                contractor_delay += de.net_delay_days

    # count CP shifts and windows with delays
    cp_shift_count = sum(
        1 for w in windows if getattr(w, "critical_path_changed", False)
    )

    windows_with_owner = sum(
        1 for w in windows if any(
            (de.responsible_party == "OWNER" and not de.is_concurrent)
            for de in w.delay_events
        )
    )
    windows_with_contractor = sum(
        1 for w in windows if any(
            (de.responsible_party == "CONTRACTOR" and not de.is_concurrent)
            for de in w.delay_events
        )
    )
    windows_with_concurrent = sum(
        1 for w in windows if any(de.is_concurrent for de in w.delay_events)
    )

    summary = {
        "baseline_finish": comparison_result.baseline_finish,
        "current_finish": comparison_result.current_finish,
        "total_delay_days": comparison_result.total_delay_days,
        "owner_delay_days": owner_delay,
        "contractor_delay_days": contractor_delay,
        "concurrent_delay_days": concurrent_delay,
        "spi": comparison_result.spi_overall,
        "num_windows": len(windows),
        "cp_shift_count": cp_shift_count,
        "windows_with_owner_delay": windows_with_owner,
        "windows_with_contractor_delay": windows_with_contractor,
        "windows_with_concurrent_delay": windows_with_concurrent,
        "validation_issue_count": len(validation_issues),
    }

    # optional: simple dicts for each window if you want to show them in UI
    window_summaries = []
    for w in windows:
        window_summaries.append({
            "name": w.name,
            "start": w.start_date,
            "end": w.end_date,
            "net_delay_days": w.net_delay_days,
            "owner_delay_days": w.owner_delay_days,
            "contractor_delay_days": w.contractor_delay_days,
            "concurrent_delay_days": w.concurrent_delay_days,
            "critical_path_changed": getattr(w, "critical_path_changed", False),
        })

    return {
        "summary": summary,
        "window_summaries": window_summaries,
        "excel_path": excel_path,
        "pdf_path": pdf_path,
    }