"""
Schedule Parser for P6 XER Files

Parses Primavera P6 XER files using the xerparser library and maps the data
to custom Schedule, Activity, and Relationship models.

Features:
- Full XER file parsing with xerparser
- Critical path calculation with relationship types (FS, SS, FF, SF) and lags
- Calendar-aware duration calculations
- Robust error handling and logging

Limitations:
- Calendar non-working days are not fully accounted for in CPM calculations
- Custom calendar exceptions may not be reflected in float calculations
- For production use, consider validating critical path against P6 native calculations
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

try:
    import xerparser
except ImportError:
    raise ImportError(
        "xerparser library is required. Install with: pip install xerparser"
    )

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    raise ImportError(
        "python-dateutil is required. Install with: pip install python-dateutil"
    )

from models import (
    Schedule, Activity, Relationship, ActivityStatus,
    RelationshipType, Calendar, Resource, WBS
)

logger = logging.getLogger(__name__)


class ScheduleParser:
    """Parse P6 XER files using xerparser library and map to custom models."""
    
    def __init__(self):
        self.xer_reader: Optional[xerparser.reader.Reader] = None
        self.project: Optional[xerparser.model.Project] = None
        self.activities_map: Dict[str, Activity] = {}
        self.calendars_map: Dict[str, Calendar] = {}
        self.wbs_map: Dict[str, WBS] = {}
        
    def parse_xer_file(self, file_path: str) -> Schedule:
        """
        Parse XER file and return Schedule object.
        
        Args:
            file_path: Path to XER file
            
        Returns:
            Schedule object with all activities and relationships
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file cannot be parsed
        """
        try:
            logger.info(f"Starting XER file parsing: {file_path}")
            
            # Validate file exists
            if not Path(file_path).exists():
                raise FileNotFoundError(f"XER file not found: {file_path}")
            
            # Read XER file using xerparser
            self.xer_reader = xerparser.reader.Reader(file_path)
            
            # Get first project (or handle multiple projects)
            if not self.xer_reader.projects:
                raise ValueError("No projects found in XER file")
            
            self.project = self.xer_reader.projects[0]
            logger.info(f"Found project: {self.project.name}")
            
            # Parse components
            calendars = self._parse_calendars()
            wbs_items = self._parse_wbs()
            activities = self._parse_activities()
            relationships = self._parse_relationships()
            resources = self._parse_resources()
            
            # Calculate critical path and float
            self._calculate_critical_path(activities, relationships)
            
            # Create Schedule object
            schedule = Schedule(
                project_name=self.project.name,
                project_id=str(self.project.uid) if hasattr(self.project, 'uid') else self.project.name,
                data_date=self._parse_date(self.project.last_recalc_date) if hasattr(self.project, 'last_recalc_date') else datetime.now(),
                start_date=self._parse_date(self.project.start_date) if hasattr(self.project, 'start_date') else None,
                finish_date=self._parse_date(self.project.finish_date) if hasattr(self.project, 'finish_date') else None,
                activities=activities,
                relationships=relationships,
                calendars=calendars,
                wbs_items=wbs_items,
                resources=resources
            )
            
            logger.info(f"Successfully parsed {len(activities)} activities and {len(relationships)} relationships")
            return schedule
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error parsing XER file: {e}", exc_info=True)
            raise ValueError(f"Failed to parse XER file: {str(e)}")
    
    def _parse_calendars(self) -> List[Calendar]:
        """Parse calendars from XER file."""
        calendars = []
        
        try:
            for xer_cal in self.project.calendars:
                # Ensure hours_per_day is never zero
                hours_per_day = getattr(xer_cal, 'day_hr_cnt', 8.0) or 8.0
                hours_per_week = getattr(xer_cal, 'week_hr_cnt', 40.0) or 40.0
                hours_per_month = getattr(xer_cal, 'month_hr_cnt', 160.0) or 160.0
                hours_per_year = getattr(xer_cal, 'year_hr_cnt', 1920.0) or 1920.0
                
                calendar = Calendar(
                    calendar_id=str(xer_cal.uid) if hasattr(xer_cal, 'uid') else xer_cal.name,
                    name=xer_cal.name,
                    is_default=getattr(xer_cal, 'default_flag', False),
                    type=getattr(xer_cal, 'type', 'Global'),
                    hours_per_day=hours_per_day,
                    hours_per_week=hours_per_week,
                    hours_per_month=hours_per_month,
                    hours_per_year=hours_per_year
                )
                calendars.append(calendar)
                self.calendars_map[calendar.calendar_id] = calendar
                
            logger.info(f"Parsed {len(calendars)} calendars")
        except Exception as e:
            logger.warning(f"Error parsing calendars: {e}")
            
        return calendars
    
    def _parse_wbs(self) -> List[WBS]:
        """Parse WBS structure from XER file."""
        wbs_items = []
        
        try:
            for xer_wbs in self.project.wbs_nodes:
                wbs = WBS(
                    wbs_id=str(xer_wbs.uid) if hasattr(xer_wbs, 'uid') else xer_wbs.short_name,
                    wbs_code=getattr(xer_wbs, 'short_name', ''),
                    wbs_name=xer_wbs.name,
                    parent_wbs_id=str(xer_wbs.parent.uid) if xer_wbs.parent and hasattr(xer_wbs.parent, 'uid') else None,
                    level=getattr(xer_wbs, 'seq_num', 0)
                )
                wbs_items.append(wbs)
                self.wbs_map[wbs.wbs_id] = wbs
                
            logger.info(f"Parsed {len(wbs_items)} WBS items")
        except Exception as e:
            logger.warning(f"Error parsing WBS: {e}")
            
        return wbs_items
    
    def _parse_activities(self) -> List[Activity]:
        """Parse activities/tasks from XER file."""
        activities = []
        
        try:
            for xer_task in self.project.tasks:
                # Map activity status
                status = self._map_activity_status(xer_task)
                
                # Get calendar with safeguard
                calendar_id = None
                calendar = None
                if hasattr(xer_task, 'calendar') and xer_task.calendar:
                    calendar_id = str(xer_task.calendar.uid) if hasattr(xer_task.calendar, 'uid') else xer_task.calendar.name
                    calendar = self.calendars_map.get(calendar_id)
                
                # Safe hours_per_day with fallback
                hours_per_day = 8.0
                if calendar:
                    hours_per_day = getattr(calendar, 'hours_per_day', 8.0) or 8.0
                
                # Get WBS
                wbs_code = None
                wbs_name = None
                if hasattr(xer_task, 'wbs') and xer_task.wbs:
                    wbs_code = getattr(xer_task.wbs, 'short_name', '')
                    wbs_name = xer_task.wbs.name
                
                # Parse dates
                start_date = self._parse_date(xer_task.start_date) if hasattr(xer_task, 'start_date') else None
                finish_date = self._parse_date(xer_task.finish_date) if hasattr(xer_task, 'finish_date') else None
                actual_start = self._parse_date(xer_task.act_start_date) if hasattr(xer_task, 'act_start_date') else None
                actual_finish = self._parse_date(xer_task.act_end_date) if hasattr(xer_task, 'act_end_date') else None
                early_start = self._parse_date(xer_task.early_start_date) if hasattr(xer_task, 'early_start_date') else start_date
                early_finish = self._parse_date(xer_task.early_end_date) if hasattr(xer_task, 'early_end_date') else finish_date
                late_start = self._parse_date(xer_task.late_start_date) if hasattr(xer_task, 'late_start_date') else start_date
                late_finish = self._parse_date(xer_task.late_end_date) if hasattr(xer_task, 'late_end_date') else finish_date
                
                # Get duration (convert hours to days with safeguard)
                target_duration_hrs = getattr(xer_task, 'target_drtn_hr_cnt', 0.0) or 0.0
                remain_duration_hrs = getattr(xer_task, 'remain_drtn_hr_cnt', 0.0) or 0.0
                
                duration = target_duration_hrs / hours_per_day
                remaining_duration = remain_duration_hrs / hours_per_day
                
                # Get float values (convert hours to days)
                total_float_hrs = getattr(xer_task, 'total_float_hr_cnt', 0.0)
                total_float = (total_float_hrs / hours_per_day) if total_float_hrs is not None else 0.0
                
                free_float_hrs = getattr(xer_task, 'free_float_hr_cnt', 0.0)
                free_float = (free_float_hrs / hours_per_day) if free_float_hrs is not None else 0.0
                
                # Determine if critical
                is_critical = total_float <= 0.0 or getattr(xer_task, 'crit_path_flag', False)
                
                # Create Activity object
                activity = Activity(
                    activity_id=str(xer_task.uid) if hasattr(xer_task, 'uid') else xer_task.task_code,
                    activity_code=xer_task.task_code,
                    activity_name=xer_task.name,
                    wbs=wbs_code,
                    wbs_name=wbs_name,
                    calendar_id=calendar_id,
                    activity_type=getattr(xer_task, 'task_type', 'Task Dependent'),
                    duration=duration,
                    original_duration=duration,
                    remaining_duration=remaining_duration,
                    start_date=start_date,
                    finish_date=finish_date,
                    actual_start=actual_start,
                    actual_finish=actual_finish,
                    early_start=early_start,
                    early_finish=early_finish,
                    late_start=late_start,
                    late_finish=late_finish,
                    total_float=total_float,
                    free_float=free_float,
                    is_critical=is_critical,
                    is_milestone=getattr(xer_task, 'task_type', '') == 'TT_Mile' or (target_duration_hrs == 0),
                    status=status,
                    percent_complete=getattr(xer_task, 'phys_complete_pct', 0.0),
                    predecessors=[],
                    successors=[],
                    resources=[]
                )
                
                activities.append(activity)
                self.activities_map[activity.activity_id] = activity
                
            logger.info(f"Parsed {len(activities)} activities")
        except Exception as e:
            logger.error(f"Error parsing activities: {e}", exc_info=True)
            raise
            
        return activities
    
    def _parse_relationships(self) -> List[Relationship]:
        """Parse task relationships/dependencies from XER file."""
        relationships = []
        
        try:
            # Try to parse from task_pred table if available
            if hasattr(self.xer_reader, 'task_pred') and self.xer_reader.task_pred:
                for pred_rel in self.xer_reader.task_pred:
                    try:
                        pred_id = str(pred_rel.pred_task_id) if hasattr(pred_rel, 'pred_task_id') else None
                        succ_id = str(pred_rel.task_id) if hasattr(pred_rel, 'task_id') else None
                        
                        if not pred_id or not succ_id:
                            continue
                        
                        # Map relationship type
                        rel_type = self._map_relationship_type(pred_rel)
                        
                        # Get lag (convert hours to days using calendar)
                        lag_hrs = getattr(pred_rel, 'lag_hr_cnt', 0.0) or 0.0
                        
                        # Use successor's calendar for lag calculation
                        hours_per_day = 8.0
                        if succ_id in self.activities_map:
                            succ_activity = self.activities_map[succ_id]
                            if succ_activity.calendar_id and succ_activity.calendar_id in self.calendars_map:
                                calendar = self.calendars_map[succ_activity.calendar_id]
                                hours_per_day = getattr(calendar, 'hours_per_day', 8.0) or 8.0
                        
                        lag = lag_hrs / hours_per_day
                        
                        relationship = Relationship(
                            predecessor_id=pred_id,
                            successor_id=succ_id,
                            relationship_type=rel_type,
                            lag=lag
                        )
                        relationships.append(relationship)
                        
                        # Update activity predecessor/successor lists
                        if succ_id in self.activities_map:
                            self.activities_map[succ_id].predecessors.append(pred_id)
                        if pred_id in self.activities_map:
                            self.activities_map[pred_id].successors.append(succ_id)
                            
                    except Exception as e:
                        logger.warning(f"Error parsing relationship: {e}")
                        continue
            else:
                # Fallback to parsing from task.predecessors
                for xer_task in self.project.tasks:
                    task_id = str(xer_task.uid) if hasattr(xer_task, 'uid') else xer_task.task_code
                    
                    if hasattr(xer_task, 'predecessors'):
                        for pred_rel in xer_task.predecessors:
                            try:
                                pred_task = pred_rel.predecessor_task
                                pred_id = str(pred_task.uid) if hasattr(pred_task, 'uid') else pred_task.task_code
                                
                                # Map relationship type
                                rel_type = self._map_relationship_type(pred_rel)
                                
                                # Get lag with calendar
                                lag_hrs = getattr(pred_rel, 'lag_hr_cnt', 0.0) or 0.0
                                hours_per_day = 8.0
                                if task_id in self.activities_map:
                                    act = self.activities_map[task_id]
                                    if act.calendar_id and act.calendar_id in self.calendars_map:
                                        cal = self.calendars_map[act.calendar_id]
                                        hours_per_day = getattr(cal, 'hours_per_day', 8.0) or 8.0
                                
                                lag = lag_hrs / hours_per_day
                                
                                relationship = Relationship(
                                    predecessor_id=pred_id,
                                    successor_id=task_id,
                                    relationship_type=rel_type,
                                    lag=lag
                                )
                                relationships.append(relationship)
                                
                                # Update activity lists
                                if task_id in self.activities_map:
                                    self.activities_map[task_id].predecessors.append(pred_id)
                                if pred_id in self.activities_map:
                                    self.activities_map[pred_id].successors.append(task_id)
                                    
                            except Exception as e:
                                logger.warning(f"Error parsing predecessor for task {task_id}: {e}")
                                continue
            
            logger.info(f"Parsed {len(relationships)} relationships")
        except Exception as e:
            logger.warning(f"Error parsing relationships: {e}")
            
        return relationships
    
    def _parse_resources(self) -> List[Resource]:
        """Parse resources from XER file."""
        resources = []
        
        try:
            if hasattr(self.project, 'resources'):
                for xer_rsrc in self.project.resources:
                    resource = Resource(
                        resource_id=str(xer_rsrc.uid) if hasattr(xer_rsrc, 'uid') else xer_rsrc.rsrc_id,
                        resource_name=xer_rsrc.rsrc_name if hasattr(xer_rsrc, 'rsrc_name') else '',
                        resource_type=getattr(xer_rsrc, 'rsrc_type', 'Labor')
                    )
                    resources.append(resource)
                    
            logger.info(f"Parsed {len(resources)} resources")
        except Exception as e:
            logger.warning(f"Error parsing resources: {e}")
            
        return resources
    
    def _map_activity_status(self, xer_task) -> ActivityStatus:
        """Map xerparser task status to ActivityStatus enum."""
        if not hasattr(xer_task, 'status_code'):
            return ActivityStatus.NOT_STARTED
        
        status_code = xer_task.status_code.upper()
        
        status_mapping = {
            'TK_NotStart': ActivityStatus.NOT_STARTED,
            'TK_Active': ActivityStatus.IN_PROGRESS,
            'TK_Complete': ActivityStatus.COMPLETED
        }
        
        return status_mapping.get(status_code, ActivityStatus.NOT_STARTED)
    
    def _map_relationship_type(self, pred_rel) -> RelationshipType:
        """Map xerparser relationship type to RelationshipType enum."""
        if not hasattr(pred_rel, 'pred_type'):
            return RelationshipType.FINISH_TO_START
        
        type_code = pred_rel.pred_type.upper()
        
        type_mapping = {
            'PR_FS': RelationshipType.FINISH_TO_START,
            'PR_FF': RelationshipType.FINISH_TO_FINISH,
            'PR_SS': RelationshipType.START_TO_START,
            'PR_SF': RelationshipType.START_TO_FINISH
        }
        
        return type_mapping.get(type_code, RelationshipType.FINISH_TO_START)
    
    def _parse_date(self, date_value) -> Optional[datetime]:
        """
        Parse date from various formats using dateutil.parser.
        
        Args:
            date_value: Date string or datetime object
            
        Returns:
            Parsed datetime or None if parsing fails
        """
        if date_value is None:
            return None
        
        if isinstance(date_value, datetime):
            return date_value
        
        if isinstance(date_value, str):
            try:
                return dateutil_parser.parse(date_value)
            except Exception as e:
                logger.warning(f"Could not parse date '{date_value}': {e}")
                return None
        
        return None
    
    def _calculate_critical_path(self, activities: List[Activity], 
                                relationships: List[Relationship]) -> None:
        """
        Calculate critical path and update float values.
        Uses forward and backward pass algorithms with relationship types and lags.
        
        Note: This calculation does not account for calendar non-working days.
        For production use, validate against P6 native calculations.
        
        Args:
            activities: List of activities
            relationships: List of relationships with types and lags
        """
        try:
            logger.info("Calculating critical path...")
            logger.warning("CPM calculation does not account for calendar non-working days")
            
            # Create activity and relationship lookups
            activity_dict = {act.activity_id: act for act in activities}
            rel_dict: Dict[str, List[Relationship]] = {}
            
            for rel in relationships:
                if rel.successor_id not in rel_dict:
                    rel_dict[rel.successor_id] = []
                rel_dict[rel.successor_id].append(rel)
            
            # Forward pass - calculate early dates
            self._forward_pass_with_relationships(activities, activity_dict, rel_dict)
            
            # Backward pass - calculate late dates
            self._backward_pass_with_relationships(activities, activity_dict, rel_dict)
            
            # Calculate float and identify critical activities
            for activity in activities:
                if activity.early_start and activity.late_start:
                    activity.total_float = (activity.late_start - activity.early_start).days
                    activity.is_critical = activity.total_float <= 0
                    
                if activity.early_finish and activity.late_finish:
                    finish_float = (activity.late_finish - activity.early_finish).days
                    activity.total_float = min(activity.total_float, finish_float)
            
            critical_count = sum(1 for act in activities if act.is_critical)
            logger.info(f"Critical path calculated: {critical_count} critical activities")
            
        except Exception as e:
            logger.warning(f"Error calculating critical path: {e}")
    
    def _forward_pass_with_relationships(self, activities: List[Activity], 
                                        activity_dict: Dict[str, Activity],
                                        rel_dict: Dict[str, List[Relationship]]) -> None:
        """
        Forward pass to calculate early start and early finish dates.
        Handles all relationship types (FS, SS, FF, SF) and lags.
        """
        # Find start activities (no predecessors)
        start_activities = [act for act in activities if not act.predecessors]
        
        # Initialize early dates for start activities
        for act in start_activities:
            if not act.early_start and act.start_date:
                act.early_start = act.start_date
            if not act.early_finish and act.early_start:
                act.early_finish = act.early_start + timedelta(days=act.duration)
        
        # Process activities in topological order
        visited = set()
        
        def visit(activity: Activity):
            if activity.activity_id in visited:
                return
            
            # Visit all predecessors first
            for pred_id in activity.predecessors:
                if pred_id in activity_dict:
                    visit(activity_dict[pred_id])
            
            # Calculate early start based on predecessors with relationship types
            if activity.activity_id in rel_dict:
                max_early_date = None
                
                for rel in rel_dict[activity.activity_id]:
                    if rel.predecessor_id not in activity_dict:
                        continue
                    
                    pred = activity_dict[rel.predecessor_id]
                    constraint_date = self._calculate_constraint_date(
                        pred, rel.relationship_type, rel.lag, is_forward=True
                    )
                    
                    if constraint_date:
                        if max_early_date is None or constraint_date > max_early_date:
                            max_early_date = constraint_date
                
                if max_early_date:
                    activity.early_start = max_early_date
                    activity.early_finish = activity.early_start + timedelta(days=activity.duration)
            
            visited.add(activity.activity_id)
        
        for act in activities:
            visit(act)
    
    def _backward_pass_with_relationships(self, activities: List[Activity],
                                         activity_dict: Dict[str, Activity],
                                         rel_dict: Dict[str, List[Relationship]]) -> None:
        """
        Backward pass to calculate late start and late finish dates.
        Handles all relationship types (FS, SS, FF, SF) and lags.
        """
        # Find finish activities (no successors)
        finish_activities = [act for act in activities if not act.successors]
        
        # Initialize late dates for finish activities
        for act in finish_activities:
            if not act.late_finish and act.early_finish:
                act.late_finish = act.early_finish
            if not act.late_start and act.late_finish:
                act.late_start = act.late_finish - timedelta(days=act.duration)
        
        # Create reverse relationship lookup
        reverse_rel_dict: Dict[str, List[Relationship]] = {}
        for succ_id, rels in rel_dict.items():
            for rel in rels:
                if rel.predecessor_id not in reverse_rel_dict:
                    reverse_rel_dict[rel.predecessor_id] = []
                reverse_rel_dict[rel.predecessor_id].append(rel)
        
        # Process activities in reverse topological order
        visited = set()
        
        def visit(activity: Activity):
            if activity.activity_id in visited:
                return
            
            # Visit all successors first
            for succ_id in activity.successors:
                if succ_id in activity_dict:
                    visit(activity_dict[succ_id])
            
            # Calculate late finish based on successors
            if activity.activity_id in reverse_rel_dict:
                min_late_date = None
                
                for rel in reverse_rel_dict[activity.activity_id]:
                    if rel.successor_id not in activity_dict:
                        continue
                    
                    succ = activity_dict[rel.successor_id]
                    constraint_date = self._calculate_constraint_date(
                        succ, rel.relationship_type, rel.lag, is_forward=False
                    )
                    
                    if constraint_date:
                        if min_late_date is None or constraint_date < min_late_date:
                            min_late_date = constraint_date
                
                if min_late_date:
                    activity.late_finish = min_late_date
                    activity.late_start = activity.late_finish - timedelta(days=activity.duration)
            
            visited.add(activity.activity_id)
        
        for act in reversed(activities):
            visit(act)
    
    def _calculate_constraint_date(self, activity: Activity, 
                                  rel_type: RelationshipType,
                                  lag: float,
                                  is_forward: bool) -> Optional[datetime]:
        """
        Calculate constraint date based on relationship type and lag.
        
        Args:
            activity: Predecessor (forward pass) or successor (backward pass)
            rel_type: Relationship type (FS, SS, FF, SF)
            lag: Lag in days
            is_forward: True for forward pass, False for backward pass
            
        Returns:
            Constraint date
        """
        lag_delta = timedelta(days=lag)
        
        if is_forward:
            # Forward pass: calculate early date for successor
            if rel_type == RelationshipType.FINISH_TO_START:
                return activity.early_finish + lag_delta if activity.early_finish else None
            elif rel_type == RelationshipType.START_TO_START:
                return activity.early_start + lag_delta if activity.early_start else None
            elif rel_type == RelationshipType.FINISH_TO_FINISH:
                return (activity.early_finish + lag_delta - timedelta(days=0)) if activity.early_finish else None
            elif rel_type == RelationshipType.START_TO_FINISH:
                return (activity.early_start + lag_delta - timedelta(days=0)) if activity.early_start else None
        else:
            # Backward pass: calculate late date for predecessor
            if rel_type == RelationshipType.FINISH_TO_START:
                return activity.late_start - lag_delta if activity.late_start else None
            elif rel_type == RelationshipType.START_TO_START:
                return (activity.late_start - lag_delta + timedelta(days=0)) if activity.late_start else None
            elif rel_type == RelationshipType.FINISH_TO_FINISH:
                return activity.late_finish - lag_delta if activity.late_finish else None
            elif rel_type == RelationshipType.START_TO_FINISH:
                return (activity.late_finish - lag_delta + timedelta(days=0)) if activity.late_finish else None
        
        return None


def parse_schedule(file_path: str) -> Schedule:
    """
    Convenience function to parse a schedule file.
    
    Args:
        file_path: Path to XER file
        
    Returns:
        Schedule object with all activities and relationships
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file cannot be parsed
    """
    parser = ScheduleParser()
    return parser.parse_xer_file(file_path)