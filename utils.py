
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Set, Optional, Tuple
from models import Activity, Schedule, ActivityStatus
import calendar


# Configure logging
logger = logging.getLogger(__name__)


class DateUtils:
    """Utility class for date calculations."""
    
    @staticmethod
    def workday_diff(start_date: datetime, end_date: datetime, 
                     holidays: List[date] = None) -> int:
        """
        Calculate workdays between two dates excluding weekends and holidays.
        
        Args:
            start_date: Start date
            end_date: End date
            holidays: List of holiday dates
            
        Returns:
            Number of workdays
        """
        if holidays is None:
            holidays = []
        
        workdays = 0
        current_date = start_date.date() if isinstance(start_date, datetime) else start_date
        end = end_date.date() if isinstance(end_date, datetime) else end_date
        
        while current_date <= end:
            # Check if it's a weekday and not a holiday
            if current_date.weekday() < 5 and current_date not in holidays:
                workdays += 1
            current_date += timedelta(days=1)
        
        return workdays
    
    @staticmethod
    def add_workdays(start_date: datetime, days: int,
                     holidays: List[date] = None) -> datetime:
        """
        Add workdays to a date excluding weekends and holidays.
        
        Args:
            start_date: Starting date
            days: Number of workdays to add
            holidays: List of holiday dates
            
        Returns:
            Resulting date
        """
        if holidays is None:
            holidays = []
        
        current_date = start_date
        days_added = 0
        
        while days_added < days:
            current_date += timedelta(days=1)
            if (current_date.weekday() < 5 and 
                current_date.date() not in holidays):
                days_added += 1
        
        return current_date
    
    @staticmethod
    def get_month_windows(start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime]]:
        """
        Generate monthly windows between two dates.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of (window_start, window_end) tuples
        """
        windows = []
        current_date = start_date
        
        while current_date < end_date:
            # Get last day of current month
            last_day = calendar.monthrange(current_date.year, current_date.month)[1]
            month_end = datetime(current_date.year, current_date.month, last_day, 23, 59, 59)
            
            # Use end_date if it's before month end
            window_end = min(month_end, end_date)
            
            windows.append((current_date, window_end))
            
            # Move to first day of next month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        return windows


class FloatCalculator:
    """Utility class for float calculations."""
    
    @staticmethod
    def calculate_total_float(activity: Activity, project_finish: datetime,
                              successors: Dict[str, Activity]) -> float:
        """
        Calculate total float for an activity.
        
        Total Float = Late Finish - Early Finish
        
        Args:
            activity: Activity to calculate float for
            project_finish: Project finish date
            successors: Dictionary of successor activities
            
        Returns:
            Total float in days
        """
        if not activity.finish_date:
            return 0.0
        
        # If activity has no successors, float is to project end
        if not activity.successors:
            return (project_finish - activity.finish_date).days
        
        # Find earliest successor start
        earliest_successor_start = project_finish
        for succ_id in activity.successors:
            if succ_id in successors and successors[succ_id].start_date:
                earliest_successor_start = min(
                    earliest_successor_start,
                    successors[succ_id].start_date
                )
        
        return (earliest_successor_start - activity.finish_date).days
    
    @staticmethod
    def calculate_free_float(activity: Activity,
                            successors: Dict[str, Activity]) -> float:
        """
        Calculate free float for an activity.
        
        Free Float = Earliest Successor Start - Activity Finish
        
        Args:
            activity: Activity to calculate float for
            successors: Dictionary of successor activities
            
        Returns:
            Free float in days
        """
        if not activity.finish_date or not activity.successors:
            return 0.0
        
        # Find earliest successor start
        earliest_successor_start = None
        for succ_id in activity.successors:
            if succ_id in successors and successors[succ_id].start_date:
                if earliest_successor_start is None:
                    earliest_successor_start = successors[succ_id].start_date
                else:
                    earliest_successor_start = min(
                        earliest_successor_start,
                        successors[succ_id].start_date
                    )
        
        if earliest_successor_start:
            return (earliest_successor_start - activity.finish_date).days
        
        return 0.0


class CriticalPathAnalyzer:
    """Utility class for critical path identification."""
    
    @staticmethod
    def find_critical_path(schedule: Schedule,
                          float_threshold: float = 0.0) -> List[Activity]:
        """
        Identify critical path activities in schedule.
        
        Critical activities have total float <= threshold.
        
        Args:
            schedule: Schedule to analyze
            float_threshold: Float threshold for criticality
            
        Returns:
            List of critical path activities
        """
        critical_activities = []
        
        for activity in schedule.activities.values():
            if activity.total_float <= float_threshold:
                activity.is_critical = True
                critical_activities.append(activity)
        
        return critical_activities
    
    @staticmethod
    def get_critical_chain(activity: Activity, schedule: Schedule,
                          direction: str = "forward") -> List[Activity]:
        """
        Get chain of critical activities from given activity.
        
        Args:
            activity: Starting activity
            schedule: Schedule containing activities
            direction: "forward" for successors, "backward" for predecessors
            
        Returns:
            List of activities in critical chain
        """
        chain = [activity]
        visited = {activity.activity_id}
        
        if direction == "forward":
            neighbors_key = "successors"
        else:
            neighbors_key = "predecessors"
        
        current = activity
        while True:
            next_critical = None
            neighbors = getattr(current, neighbors_key, [])
            
            for neighbor_id in neighbors:
                if neighbor_id in visited:
                    continue
                
                neighbor = schedule.activities.get(neighbor_id)
                if neighbor and neighbor.is_critical:
                    next_critical = neighbor
                    visited.add(neighbor_id)
                    break
            
            if next_critical:
                if direction == "forward":
                    chain.append(next_critical)
                else:
                    chain.insert(0, next_critical)
                current = next_critical
            else:
                break
        
        return chain


class ScheduleValidator:
    """Utility class for schedule validation."""
    
    @staticmethod
    def validate_schedule(schedule: Schedule) -> Dict[str, List[str]]:
        """
        Validate schedule data quality.
        
        Checks for:
        - Missing dates
        - Logic errors
        - Out-of-sequence activities
        - Negative float anomalies
        - Missing relationships
        
        Args:
            schedule: Schedule to validate
            
        Returns:
            Dictionary of validation issues by category
        """
        issues = {
            'missing_dates': [],
            'logic_errors': [],
            'out_of_sequence': [],
            'negative_float': [],
            'missing_relationships': [],
            'constraint_issues': []
        }
        
        for act_id, activity in schedule.activities.items():
            # Check for missing dates
            if activity.status != ActivityStatus.NOT_STARTED:
                if not activity.actual_start:
                    issues['missing_dates'].append(
                        f"{act_id}: Started activity missing actual start"
                    )
            
            if activity.status == ActivityStatus.COMPLETED:
                if not activity.actual_finish:
                    issues['missing_dates'].append(
                        f"{act_id}: Completed activity missing actual finish"
                    )
            
            # Check for logic errors
            if activity.start_date and activity.finish_date:
                if activity.start_date > activity.finish_date:
                    issues['logic_errors'].append(
                        f"{act_id}: Start date after finish date"
                    )
            
            # Check for out-of-sequence activities
            if activity.actual_start and activity.start_date:
                if activity.actual_start < activity.start_date:
                    # Activity started before planned
                    if activity.status == ActivityStatus.IN_PROGRESS:
                        has_incomplete_preds = False
                        for pred_id in activity.predecessors:
                            pred = schedule.activities.get(pred_id)
                            if pred and pred.status != ActivityStatus.COMPLETED:
                                has_incomplete_preds = True
                                break
                        
                        if has_incomplete_preds:
                            issues['out_of_sequence'].append(
                                f"{act_id}: Started with incomplete predecessors"
                            )
            
            # Check for unusual negative float
            if activity.total_float < -30:  # More than 30 days negative
                issues['negative_float'].append(
                    f"{act_id}: Excessive negative float ({activity.total_float} days)"
                )
            
            # Check for missing relationships
            if not activity.is_milestone:
                if not activity.predecessors and not activity.successors:
                    issues['missing_relationships'].append(
                        f"{act_id}: Activity has no relationships"
                    )
        
        return issues
    
    @staticmethod
    def identify_out_of_sequence(schedule: Schedule) -> List[Activity]:
        """
        Identify activities started out of sequence.
        
        Args:
            schedule: Schedule to analyze
            
        Returns:
            List of out-of-sequence activities
        """
        oos_activities = []
        
        for activity in schedule.activities.values():
            if activity.status in [ActivityStatus.IN_PROGRESS, ActivityStatus.COMPLETED]:
                if activity.actual_start:
                    # Check if any predecessors were incomplete at activity start
                    for pred_id in activity.predecessors:
                        pred = schedule.activities.get(pred_id)
                        if pred:
                            if not pred.actual_finish:
                                oos_activities.append(activity)
                                break
                            elif pred.actual_finish > activity.actual_start:
                                oos_activities.append(activity)
                                break
        
        return oos_activities


