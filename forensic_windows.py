
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from models import (
    Schedule, Activity, ForensicWindow, DelayEvent, DelayType,
    ComparisonResult, ActivityStatus
)
from utils import DateUtils
from analysis_engine import AnalysisEngine

# Configure logging
logger = logging.getLogger(__name__)


class ForensicWindowAnalyzer:
    """
    Forensic schedule analysis using window methodology.
    
    This class implements various forensic delay analysis techniques
    to identify and quantify delays in construction schedules.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize forensic window analyzer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.windows: List[ForensicWindow] = []
        self.analysis_engine = AnalysisEngine(config)
        logger.info("Forensic window analyzer initialized")
    
    def create_monthly_windows(self, start_date: datetime,
                               end_date: datetime) -> List[ForensicWindow]:
        """
        Create monthly forensic analysis windows.
        
        Args:
            start_date: Analysis start date
            end_date: Analysis end date
            
        Returns:
            List of ForensicWindow objects
        """
        logger.info(f"Creating monthly windows from {start_date} to {end_date}")
        
        windows = []
        month_periods = DateUtils.get_month_windows(start_date, end_date)
        
        for i, (window_start, window_end) in enumerate(month_periods):
            window_id = f"WINDOW_{i+1:03d}_{window_start.strftime('%Y%m')}"
            
            window = ForensicWindow(
                window_id=window_id,
                start_date=window_start,
                end_date=window_end
            )
            
            windows.append(window)
        
        self.windows = windows
        logger.info(f"Created {len(windows)} monthly windows")
        
        return windows
    
    def create_custom_windows(self, start_date: datetime, end_date: datetime,
                             period_days: int) -> List[ForensicWindow]:
        """
        Create custom-period forensic windows.
        
        Args:
            start_date: Analysis start date
            end_date: Analysis end date
            period_days: Days per window period
            
        Returns:
            List of ForensicWindow objects
        """
        logger.info(f"Creating {period_days}-day windows")
        
        windows = []
        current_date = start_date
        window_num = 1
        
        while current_date < end_date:
            window_end = min(current_date + timedelta(days=period_days), end_date)
            
            window_id = f"WINDOW_{window_num:03d}_{current_date.strftime('%Y%m%d')}"
            
            window = ForensicWindow(
                window_id=window_id,
                start_date=current_date,
                end_date=window_end
            )
            
            windows.append(window)
            
            current_date = window_end
            window_num += 1
        
        self.windows = windows
        logger.info(f"Created {len(windows)} custom windows")
        
        return windows
    
    def analyze_window(self, window: ForensicWindow,
                       baseline_schedule: Schedule,
                       current_schedule: Schedule) -> ForensicWindow:
        """
        Analyze a single forensic window.
        
        Args:
            window: ForensicWindow to analyze
            baseline_schedule: Baseline schedule
            current_schedule: Current schedule
            
        Returns:
            Updated ForensicWindow with analysis results
        """
        logger.info(f"Analyzing window: {window.window_id}")
        
        window.baseline_schedule = baseline_schedule
        window.current_schedule = current_schedule
        
        # Set schedules in analysis engine
        self.analysis_engine.baseline_schedule = baseline_schedule
        self.analysis_engine.current_schedule = current_schedule
        
        # Compare schedules
        comparison = self.analysis_engine.compare_schedules()
        
        # Identify delays in this window
        window_delays = self._identify_window_delays(
            window, comparison
        )
        window.delays = window_delays
        
        # Analyze critical path changes
        window.critical_path_changes = self._analyze_critical_path_changes(
            window, comparison
        )
        
        # Calculate float changes
        window.float_changes = comparison.float_changes
        
        logger.info(f"Window {window.window_id}: {len(window_delays)} delays identified")
        
        return window
    
    def _identify_window_delays(self, window: ForensicWindow,
                                comparison: ComparisonResult) -> List[DelayEvent]:
        """
        Identify delays that occurred within a specific window.
        
        Args:
            window: ForensicWindow being analyzed
            comparison: ComparisonResult from schedule comparison
            
        Returns:
            List of DelayEvents in this window
        """
        window_delays = []
        
        for activity in comparison.delayed_activities:
            baseline_act = comparison.baseline_schedule.activities.get(activity.activity_id)
            if not baseline_act:
                continue
            
            # Check if delay occurred in this window
            if self._is_delay_in_window(baseline_act, activity, window):
                delay_days = self.analysis_engine._calculate_activity_delay(
                    baseline_act, activity
                )
                
                # Determine impact on project
                impact = 0.0
                if activity.is_critical:
                    impact = delay_days
                
                event = DelayEvent(
                    activity_id=activity.activity_id,
                    activity_name=activity.activity_name,
                    delay_days=delay_days,
                    delay_type=DelayType.UNKNOWN,
                    start_date=baseline_act.start_date or window.start_date,
                    end_date=activity.finish_date or window.end_date,
                    impact_on_project=impact,
                    window_id=window.window_id
                )
                
                window_delays.append(event)
        
        return window_delays
    
    def _is_delay_in_window(self, baseline_act: Activity,
                           current_act: Activity,
                           window: ForensicWindow) -> bool:
        """
        Check if a delay occurred within a forensic window.
        
        Args:
            baseline_act: Baseline activity
            current_act: Current activity
            window: ForensicWindow to check
            
        Returns:
            True if delay occurred in window
        """
        # Check if activity was scheduled to complete in this window
        if baseline_act.finish_date:
            if window.start_date <= baseline_act.finish_date <= window.end_date:
                return True
        
        # Check if activity actually completed in this window
        if current_act.actual_finish:
            if window.start_date <= current_act.actual_finish <= window.end_date:
                return True
        
        # Check if activity was active during window
        if baseline_act.start_date and baseline_act.finish_date:
            if (baseline_act.start_date <= window.end_date and
                baseline_act.finish_date >= window.start_date):
                return True
        
        return False
    
    def _analyze_critical_path_changes(self, window: ForensicWindow,
                                       comparison: ComparisonResult) -> Dict[str, Any]:
        """
        Analyze how critical path changed within a window.
        
        Args:
            window: ForensicWindow being analyzed
            comparison: ComparisonResult
            
        Returns:
            Dictionary of critical path changes
        """
        changes = {
            'new_critical': [],
            'removed_critical': [],
            'remained_critical': [],
            'critical_delays': []
        }
        
        for act in comparison.new_critical_activities:
            if self._activity_in_window(act, window):
                changes['new_critical'].append({
                    'activity_id': act.activity_id,
                    'activity_name': act.activity_name,
                    'baseline_float': comparison.baseline_schedule.activities[act.activity_id].total_float,
                    'current_float': act.total_float
                })
        
        for act in comparison.removed_critical_activities:
            if self._activity_in_window(act, window):
                changes['removed_critical'].append({
                    'activity_id': act.activity_id,
                    'activity_name': act.activity_name
                })
        
        # Identify activities that remained critical
        for act in window.current_schedule.activities.values():
            if act.is_critical:
                baseline_act = window.baseline_schedule.activities.get(act.activity_id)
                if baseline_act and baseline_act.is_critical:
                    changes['remained_critical'].append({
                        'activity_id': act.activity_id,
                        'activity_name': act.activity_name
                    })
        
        # Identify delays on critical path
        for delay in window.delays:
            current_act = window.current_schedule.activities.get(delay.activity_id)
            if current_act and current_act.is_critical:
                changes['critical_delays'].append(delay)
        
        return changes
    
    def _activity_in_window(self, activity: Activity,
                           window: ForensicWindow) -> bool:
        """
        Check if activity falls within window period.
        
        Args:
            activity: Activity to check
            window: ForensicWindow
            
        Returns:
            True if activity in window
        """
        if activity.start_date and activity.finish_date:
            return (activity.start_date <= window.end_date and
                   activity.finish_date >= window.start_date)
        return False
    
    def perform_time_impact_analysis(self, baseline_schedule: Schedule,
                                     delay_event: DelayEvent,
                                     impacted_schedule: Schedule) -> Dict[str, Any]:
        """
        Perform Time Impact Analysis for a specific delay event.
        
        TIA measures the impact of a delay by comparing:
        1. Schedule before delay (baseline)
        2. Schedule after adding delay
        
        Args:
            baseline_schedule: Schedule before delay
            delay_event: DelayEvent to analyze
            impacted_schedule: Schedule after delay
            
        Returns:
            Dictionary with TIA results
        """
        logger.info(f"Performing Time Impact Analysis for {delay_event.activity_id}")
        
        # Calculate project completion before and after delay
        baseline_finish = baseline_schedule.finish_date
        impacted_finish = impacted_schedule.finish_date
        
        project_impact = (impacted_finish - baseline_finish).days
        
        # Identify critical path before and after
        baseline_critical = [act for act in baseline_schedule.activities.values()
                            if act.is_critical]
        impacted_critical = [act for act in impacted_schedule.activities.values()
                            if act.is_critical]
        
        # Determine if delay affected critical path
        affected_critical = delay_event.activity_id in [
            act.activity_id for act in baseline_critical
        ]
        
        tia_result = {
            'delay_event': delay_event,
            'baseline_finish': baseline_finish,
            'impacted_finish': impacted_finish,
            'project_impact_days': project_impact,
            'affected_critical_path': affected_critical,
            'baseline_critical_activities': len(baseline_critical),
            'impacted_critical_activities': len(impacted_critical),
            'is_excusable': None,  # To be determined by user
            'is_compensable': None  # To be determined by user
        }
        
        logger.info(f"TIA complete. Project impact: {project_impact} days")
        
        return tia_result
    
    def perform_as_planned_vs_as_built(self, baseline_schedule: Schedule,
                                       as_built_schedule: Schedule) -> Dict[str, Any]:
        """
        Perform As-Planned vs As-Built analysis.
        
        Compares the original baseline plan against actual construction.
        
        Args:
            baseline_schedule: Original baseline schedule
            as_built_schedule: As-built schedule with actuals
            
        Returns:
            Dictionary with analysis results
        """
        logger.info("Performing As-Planned vs As-Built analysis")
        
        self.analysis_engine.baseline_schedule = baseline_schedule
        self.analysis_engine.current_schedule = as_built_schedule
        
        comparison = self.analysis_engine.compare_schedules()
        
        # Categorize activities
        early_activities = []
        late_activities = []
        on_time_activities = []
        
        for act in as_built_schedule.activities.values():
            if act.status != ActivityStatus.COMPLETED:
                continue
            
            baseline_act = baseline_schedule.activities.get(act.activity_id)
            if not baseline_act:
                continue
            
            if act.actual_finish and baseline_act.finish_date:
                variance = (act.actual_finish - baseline_act.finish_date).days
                
                if variance < -2:  # Finished more than 2 days early
                    early_activities.append({
                        'activity': act,
                        'variance': variance
                    })
                elif variance > 2:  # Finished more than 2 days late
                    late_activities.append({
                        'activity': act,
                        'variance': variance
                    })
                else:
                    on_time_activities.append({
                        'activity': act,
                        'variance': variance
                    })
        
        # Calculate performance metrics
        total_activities = len(early_activities) + len(late_activities) + len(on_time_activities)
        
        result = {
            'comparison': comparison,
            'early_activities': early_activities,
            'late_activities': late_activities,
            'on_time_activities': on_time_activities,
            'total_activities_completed': total_activities,
            'percent_early': (len(early_activities) / total_activities * 100) if total_activities > 0 else 0,
            'percent_late': (len(late_activities) / total_activities * 100) if total_activities > 0 else 0,
            'percent_on_time': (len(on_time_activities) / total_activities * 100) if total_activities > 0 else 0,
            'average_delay': sum(item['variance'] for item in late_activities) / len(late_activities) if late_activities else 0,
            'total_project_delay': comparison.overall_delay
        }
        
        logger.info("As-Planned vs As-Built analysis complete")
        
        return result
    
    def analyze_concurrent_delays(self, windows: List[ForensicWindow]) -> List[Dict[str, Any]]:
        """
        Analyze concurrent delays across multiple windows.
        
        Args:
            windows: List of ForensicWindow objects
            
        Returns:
            List of concurrent delay groups
        """
        logger.info("Analyzing concurrent delays across windows")
        
        concurrent_groups = []
        
        for window in windows:
            if len(window.delays) > 1:
                # Check for overlapping delays in this window
                critical_delays = window.get_critical_delays()
                
                if len(critical_delays) > 1:
                    # Group delays by time overlap
                    for i, delay1 in enumerate(critical_delays):
                        concurrent_set = [delay1]
                        
                        for delay2 in critical_delays[i+1:]:
                            # Check if delays overlap
                            if delay1.end_date and delay2.start_date:
                                if delay1.end_date >= delay2.start_date:
                                    concurrent_set.append(delay2)
                        
                        if len(concurrent_set) > 1:
                            concurrent_groups.append({
                                'window_id': window.window_id,
                                'delays': concurrent_set,
                                'total_impact': sum(d.impact_on_project for d in concurrent_set)
                            })
        
        logger.info(f"Found {len(concurrent_groups)} concurrent delay groups")
        
        return concurrent_groups
    
    def generate_window_summary(self, windows: List[ForensicWindow]) -> pd.DataFrame:
        """
        Generate summary DataFrame of window analysis results.
        
        Args:
            windows: List of analyzed ForensicWindow objects
            
        Returns:
            DataFrame with window summaries
        """
        summary_data = []
        
        for window in windows:
            summary_data.append({
                'Window ID': window.window_id,
                'Start Date': window.start_date.strftime('%Y-%m-%d'),
                'End Date': window.end_date.strftime('%Y-%m-%d'),
                'Total Delays': len(window.delays),
                'Critical Delays': len(window.get_critical_delays()),
                'Total Delay Days': window.get_total_delay(),
                'New Critical Activities': len(window.critical_path_changes.get('new_critical', [])),
                'Activities Left Critical': len(window.critical_path_changes.get('removed_critical', []))
            })
        
        return pd.DataFrame(summary_data)


