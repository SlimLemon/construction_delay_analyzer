
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from xerparser import Xer, ScheduleError
from models import (
    Activity, Schedule, ActivityStatus, DelayEvent, DelayType,
    ComparisonResult
)
from utils import (
    DateUtils, FloatCalculator, CriticalPathAnalyzer, ScheduleValidator
)


# Configure logging
logger = logging.getLogger(__name__)


class AnalysisEngine:
    """
    Core engine for schedule delay analysis.
    
    This class provides methods to parse schedules, compare them,
    and identify various types of delays and schedule changes.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize the analysis engine.
        
        Args:
            config: Configuration dictionary with analysis parameters
        """
        self.config = config
        self.baseline_schedule: Optional[Schedule] = None
        self.current_schedule: Optional[Schedule] = None
        logger.info("Analysis engine initialized")
    
    def parse_xer_file(self, file_path: str, 
                       schedule_type: str = "baseline") -> Schedule:
        """
        Parse XER or XML file using xerparser library.
        
        Args:
            file_path: Path to XER or XML file
            schedule_type: "baseline" or "current"
            
        Returns:
            Parsed Schedule object
            
        Raises:
            ScheduleError: If file cannot be parsed
        """
        logger.info(f"Parsing {schedule_type} schedule from: {file_path}")
        
        try:
            # Parse XER file using xerparser
            xer = Xer.reader(file_path)
            
            # Get first project (or handle multiple projects)
            if not xer.projects:
                raise ScheduleError("No projects found in file")
            
            project = xer.projects[0]
            
            # Create Schedule object
            schedule = Schedule(
                project_id=project.proj_short_name or project.proj_id,
                project_name=project.proj_short_name,
                data_date=project.last_recalc_date or datetime.now(),
                start_date=project.plan_start_date or datetime.now(),
                finish_date=project.scd_end_date or datetime.now(),
                file_path=file_path
            )
            
            # Parse activities
            for task in project.tasks:
                activity = self._convert_task_to_activity(task)
                schedule.activities[activity.activity_id] = activity
            
            # Parse relationships
            for rel in project.task_relationships:
                schedule.relationships.append({
                    'predecessor': rel.pred_task.task_id,
                    'successor': rel.succ_task.task_id,
                    'type': rel.link_type,
                    'lag': rel.lag_hr_cnt / 8 if rel.lag_hr_cnt else 0  # Convert hours to days
                })
            
            # Update predecessor and successor lists
            self._update_relationships(schedule)
            
            # Calculate float values
            self._calculate_float_values(schedule)
            
            # Identify critical path
            CriticalPathAnalyzer.find_critical_path(
                schedule,
                self.config.get('analysis', {}).get('critical_path_threshold', 0)
            )
            
            logger.info(f"Successfully parsed {len(schedule.activities)} activities")
            
            # Store schedule
            if schedule_type == "baseline":
                self.baseline_schedule = schedule
            else:
                self.current_schedule = schedule
            
            return schedule
            
        except Exception as e:
            logger.error(f"Error parsing schedule: {str(e)}")
            raise ScheduleError(f"Failed to parse schedule: {str(e)}")
    
    def _convert_task_to_activity(self, task) -> Activity:
        """
        Convert xerparser Task object to Activity object.
        
        Args:
            task: xerparser Task object
            
        Returns:
            Activity object
        """
        # Determine status
        status = ActivityStatus.NOT_STARTED
        if task.status == 'TK_Complete':
            status = ActivityStatus.COMPLETED
        elif task.status == 'TK_Active':
            status = ActivityStatus.IN_PROGRESS
        
        # Create Activity
        activity = Activity(
            activity_id=task.task_id,
            activity_code=task.task_code or task.task_id,
            activity_name=task.task_name or "",
            original_duration=task.target_drtn_hr_cnt / 8 if task.target_drtn_hr_cnt else 0,
            remaining_duration=task.remain_drtn_hr_cnt / 8 if task.remain_drtn_hr_cnt else 0,
            actual_duration=task.act_drtn_hr_cnt / 8 if task.act_drtn_hr_cnt else 0,
            start_date=task.early_start_date,
            finish_date=task.early_end_date,
            actual_start=task.act_start_date,
            actual_finish=task.act_end_date,
            total_float=task.total_float_hr_cnt / 8 if task.total_float_hr_cnt else 0,
            free_float=task.free_float_hr_cnt / 8 if task.free_float_hr_cnt else 0,
            status=status,
            percent_complete=task.phys_complete_pct or 0.0,
            wbs=task.wbs.wbs_short_name if task.wbs else "",
            calendar=task.clndr.clndr_name if task.clndr else "",
            constraint_type=task.cstr_type,
            constraint_date=task.cstr_date
        )
        
        return activity
    
    def _update_relationships(self, schedule: Schedule) -> None:
        """
        Update predecessor and successor lists for all activities.
        
        Args:
            schedule: Schedule to update
        """
        for rel in schedule.relationships:
            pred_id = rel['predecessor']
            succ_id = rel['successor']
            
            if pred_id in schedule.activities:
                if succ_id not in schedule.activities[pred_id].successors:
                    schedule.activities[pred_id].successors.append(succ_id)
            
            if succ_id in schedule.activities:
                if pred_id not in schedule.activities[succ_id].predecessors:
                    schedule.activities[succ_id].predecessors.append(pred_id)
    
    def _calculate_float_values(self, schedule: Schedule) -> None:
        """
        Calculate total float and free float for all activities.
        
        Args:
            schedule: Schedule to calculate floats for
        """
        calculator = FloatCalculator()
        
        for activity in schedule.activities.values():
            # Total float
            activity.total_float = calculator.calculate_total_float(
                activity,
                schedule.finish_date,
                schedule.activities
            )
            
            # Free float
            activity.free_float = calculator.calculate_free_float(
                activity,
                schedule.activities
            )
    
    def compare_schedules(self) -> ComparisonResult:
        """
        Compare baseline and current schedules to identify changes.
        
        Returns:
            ComparisonResult with detailed comparison
            
        Raises:
            ValueError: If schedules not loaded
        """
        if not self.baseline_schedule or not self.current_schedule:
            raise ValueError("Both baseline and current schedules must be loaded")
        
        logger.info("Comparing schedules...")
        
        result = ComparisonResult(
            baseline_schedule=self.baseline_schedule,
            current_schedule=self.current_schedule
        )
        
        # Find common activities
        baseline_ids = set(self.baseline_schedule.activities.keys())
        current_ids = set(self.current_schedule.activities.keys())
        common_ids = baseline_ids.intersection(current_ids)
        
        logger.info(f"Comparing {len(common_ids)} common activities")
        
        # Compare each activity
        for act_id in common_ids:
            baseline_act = self.baseline_schedule.activities[act_id]
            current_act = self.current_schedule.activities[act_id]
            
            # Check for delays
            delay_days = self._calculate_activity_delay(baseline_act, current_act)
            if delay_days > 0:
                result.delayed_activities.append(current_act)
            elif delay_days < 0:
                result.accelerated_activities.append(current_act)
            
            # Check for critical path changes
            if not baseline_act.is_critical and current_act.is_critical:
                result.new_critical_activities.append(current_act)
                # ============================================================================
# FILE: analysis_engine.py (CONTINUED)
# ============================================================================
# ... continuing from compare_schedules method ...

            elif baseline_act.is_critical and not current_act.is_critical:
                result.removed_critical_activities.append(current_act)
            
            # Track float changes
            float_change = current_act.total_float - baseline_act.total_float
            if abs(float_change) > 0.1:  # Only track significant changes
                result.float_changes[act_id] = float_change
            
            # Check milestone delays
            if baseline_act.is_milestone:
                milestone_delay = self._calculate_milestone_delay(baseline_act, current_act)
                if milestone_delay != 0:
                    result.milestone_delays[act_id] = milestone_delay
        
        # Calculate overall project delay
        result.overall_delay = (
            self.current_schedule.finish_date - self.baseline_schedule.finish_date
        ).days
        
        # Calculate Schedule Performance Index (SPI)
        result.spi = self._calculate_spi()
        
        # Calculate completion variance
        result.completion_variance = self._calculate_completion_variance()
        
        logger.info(f"Schedule comparison complete. Overall delay: {result.overall_delay} days")
        
        return result
    
    def _calculate_activity_delay(self, baseline_act: Activity, 
                                  current_act: Activity) -> float:
        """
        Calculate delay for an activity comparing baseline to current.
        
        Args:
            baseline_act: Baseline activity
            current_act: Current activity
            
        Returns:
            Delay in days (positive = delay, negative = acceleration)
        """
        # For completed activities, compare actual finish dates
        if current_act.status == ActivityStatus.COMPLETED:
            if current_act.actual_finish and baseline_act.finish_date:
                return (current_act.actual_finish - baseline_act.finish_date).days
        
        # For in-progress activities, compare current planned finish to baseline
        if current_act.status == ActivityStatus.IN_PROGRESS:
            if current_act.finish_date and baseline_act.finish_date:
                return (current_act.finish_date - baseline_act.finish_date).days
        
        # For not started activities, compare planned finish dates
        if current_act.status == ActivityStatus.NOT_STARTED:
            if current_act.finish_date and baseline_act.finish_date:
                return (current_act.finish_date - baseline_act.finish_date).days
        
        return 0.0
    
    def _calculate_milestone_delay(self, baseline_act: Activity,
                                   current_act: Activity) -> float:
        """
        Calculate milestone delay.
        
        Args:
            baseline_act: Baseline milestone
            current_act: Current milestone
            
        Returns:
            Delay in days
        """
        if current_act.actual_finish:
            # Milestone is complete, compare actual to baseline
            if baseline_act.finish_date:
                return (current_act.actual_finish - baseline_act.finish_date).days
        else:
            # Milestone not complete, compare forecasts
            if current_act.finish_date and baseline_act.finish_date:
                return (current_act.finish_date - baseline_act.finish_date).days
        
        return 0.0
    
    def _calculate_spi(self) -> float:
        """
        Calculate Schedule Performance Index.
        
        SPI = Earned Value (EV) / Planned Value (PV)
        SPI > 1.0 = ahead of schedule
        SPI < 1.0 = behind schedule
        
        Returns:
            Schedule Performance Index
        """
        if not self.baseline_schedule or not self.current_schedule:
            return 1.0
        
        total_planned_duration = 0.0
        total_earned_value = 0.0
        
        for act_id, baseline_act in self.baseline_schedule.activities.items():
            if act_id in self.current_schedule.activities:
                current_act = self.current_schedule.activities[act_id]
                
                # Planned value (baseline duration)
                planned = baseline_act.original_duration
                total_planned_duration += planned
                
                # Earned value (percent complete * baseline duration)
                earned = (current_act.percent_complete / 100.0) * planned
                total_earned_value += earned
        
        if total_planned_duration > 0:
            return total_earned_value / total_planned_duration
        
        return 1.0
    
    def _calculate_completion_variance(self) -> float:
        """
        Calculate completion percentage variance.
        
        Returns:
            Variance in completion percentage
        """
        if not self.baseline_schedule or not self.current_schedule:
            return 0.0
        
        # Calculate expected completion based on baseline
        data_date = self.current_schedule.data_date
        project_start = self.baseline_schedule.start_date
        project_finish = self.baseline_schedule.finish_date
        
        if project_finish <= project_start:
            return 0.0
        
        total_project_days = (project_finish - project_start).days
        elapsed_days = (data_date - project_start).days
        
        expected_completion = (elapsed_days / total_project_days) * 100.0
        
        # Calculate actual completion
        total_baseline_duration = sum(
            act.original_duration 
            for act in self.baseline_schedule.activities.values()
        )
        
        total_earned = sum(
            (self.current_schedule.activities.get(act_id).percent_complete / 100.0) * act.original_duration
            for act_id, act in self.baseline_schedule.activities.items()
            if act_id in self.current_schedule.activities
        )
        
        actual_completion = (total_earned / total_baseline_duration) * 100.0 if total_baseline_duration > 0 else 0.0
        
        return actual_completion - expected_completion
    
    def identify_delay_events(self, comparison: ComparisonResult) -> List[DelayEvent]:
        """
        Identify specific delay events from schedule comparison.
        
        Args:
            comparison: ComparisonResult from compare_schedules
            
        Returns:
            List of identified delay events
        """
        logger.info("Identifying delay events...")
        
        delay_events = []
        significant_threshold = self.config.get('analysis', {}).get('significant_delay_threshold', 5)
        
        for activity in comparison.delayed_activities:
            baseline_act = comparison.baseline_schedule.activities.get(activity.activity_id)
            if not baseline_act:
                continue
            
            delay_days = self._calculate_activity_delay(baseline_act, activity)
            
            if abs(delay_days) >= significant_threshold:
                # Calculate impact on project
                impact = 0.0
                if activity.is_critical:
                    impact = delay_days
                
                # Create delay event
                event = DelayEvent(
                    activity_id=activity.activity_id,
                    activity_name=activity.activity_name,
                    delay_days=delay_days,
                    delay_type=DelayType.UNKNOWN,  # Will be classified later
                    start_date=baseline_act.start_date or datetime.now(),
                    end_date=activity.finish_date,
                    impact_on_project=impact,
                    is_concurrent=False  # Will be determined in concurrent delay analysis
                )
                
                delay_events.append(event)
        
        logger.info(f"Identified {len(delay_events)} significant delay events")
        
        return delay_events
    
    def identify_concurrent_delays(self, delay_events: List[DelayEvent]) -> List[DelayEvent]:
        """
        Identify delays that occurred concurrently.
        
        Args:
            delay_events: List of delay events
            
        Returns:
            List of concurrent delay events
        """
        logger.info("Analyzing concurrent delays...")
        
        concurrent_delays = []
        
        # Sort delays by start date
        sorted_delays = sorted(delay_events, key=lambda d: d.start_date)
        
        for i, delay1 in enumerate(sorted_delays):
            for delay2 in sorted_delays[i+1:]:
                # Check if delays overlap in time
                if delay1.end_date and delay2.start_date:
                    if delay1.end_date >= delay2.start_date:
                        delay1.is_concurrent = True
                        delay2.is_concurrent = True
                        
                        if delay1 not in concurrent_delays:
                            concurrent_delays.append(delay1)
                        if delay2 not in concurrent_delays:
                            concurrent_delays.append(delay2)
        
        logger.info(f"Found {len(concurrent_delays)} concurrent delays")
        
        return concurrent_delays
    
    def validate_schedule_quality(self, schedule: Schedule) -> Dict[str, List[str]]:
        """
        Validate schedule data quality.
        
        Args:
            schedule: Schedule to validate
            
        Returns:
            Dictionary of validation issues
        """
        validator = ScheduleValidator()
        return validator.validate_schedule(schedule)
    
    def export_comparison_to_excel(self, comparison: ComparisonResult,
                                   output_path: str) -> None:
        """
        Export comparison results to Excel.
        
        Args:
            comparison: ComparisonResult to export
            output_path: Path for output Excel file
        """
        logger.info(f"Exporting comparison to Excel: {output_path}")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Overall Project Delay (days)',
                    'Schedule Performance Index (SPI)',
                    'Completion Variance (%)',
                    'Number of Delayed Activities',
                    'Number of Accelerated Activities',
                    'Activities Became Critical',
                    'Activities Left Critical Path',
                    'Milestone Delays'
                ],
                'Value': [
                    comparison.overall_delay,
                    comparison.spi,
                    comparison.completion_variance,
                    len(comparison.delayed_activities),
                    len(comparison.accelerated_activities),
                    len(comparison.new_critical_activities),
                    len(comparison.removed_critical_activities),
                    len(comparison.milestone_delays)
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Delayed activities
            if comparison.delayed_activities:
                delayed_data = []
                for act in comparison.delayed_activities:
                    baseline_act = comparison.baseline_schedule.activities[act.activity_id]
                    delayed_data.append({
                        'Activity ID': act.activity_id,
                        'Activity Name': act.activity_name,
                        'WBS': act.wbs,
                        'Baseline Finish': baseline_act.finish_date,
                        'Current Finish': act.finish_date,
                        'Delay (days)': self._calculate_activity_delay(baseline_act, act),
                        'Is Critical': act.is_critical,
                        'Status': act.status.value
                    })
                pd.DataFrame(delayed_data).to_excel(writer, sheet_name='Delayed Activities', index=False)
            
            # Float changes
            if comparison.float_changes:
                float_data = []
                for act_id, float_change in comparison.float_changes.items():
                    act = comparison.current_schedule.activities[act_id]
                    baseline_act = comparison.baseline_schedule.activities[act_id]
                    float_data.append({
                        'Activity ID': act_id,
                        'Activity Name': act.activity_name,
                        'Baseline Float': baseline_act.total_float,
                        'Current Float': act.total_float,
                        'Float Change': float_change
                    })
                pd.DataFrame(float_data).to_excel(writer, sheet_name='Float Changes', index=False)
            
            # Milestone delays
            if comparison.milestone_delays:
                milestone_data = []
                for act_id, delay in comparison.milestone_delays.items():
                    act = comparison.current_schedule.activities[act_id]
                    milestone_data.append({
                        'Activity ID': act_id,
                        'Milestone Name': act.activity_name,
                        'Delay (days)': delay,
                        'Baseline Date': comparison.baseline_schedule.activities[act_id].finish_date,
                        'Current Date': act.finish_date or act.actual_finish
                    })
                pd.DataFrame(milestone_data).to_excel(writer, sheet_name='Milestone Delays', index=False)
        
        logger.info("Excel export complete")


