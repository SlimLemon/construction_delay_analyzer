
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from enum import Enum


class ActivityStatus(Enum):
    """Enum for activity status types."""
    NOT_STARTED = "TK_NotStart"
    IN_PROGRESS = "TK_Active"
    COMPLETED = "TK_Complete"


class DelayType(Enum):
    """Enum for delay classification types."""
    EXCUSABLE = "Excusable"
    NON_EXCUSABLE = "Non-Excusable"
    COMPENSABLE = "Compensable"
    CONCURRENT = "Concurrent"
    UNKNOWN = "Unknown"


@dataclass
class Activity:
    """
    Represents a project activity with all P6 attributes.
    
    Attributes:
        activity_id: Unique activity identifier
        activity_code: Activity code from P6
        activity_name: Activity name/description
        original_duration: Original planned duration
        remaining_duration: Remaining duration
        actual_duration: Actual duration completed
        start_date: Planned start date
        finish_date: Planned finish date
        actual_start: Actual start date
        actual_finish: Actual finish date
        total_float: Total float in days
        free_float: Free float in days
        status: Activity status
        percent_complete: Percentage complete
        wbs: Work Breakdown Structure code
        predecessors: List of predecessor activity IDs
        successors: List of successor activity IDs
        resources: Assigned resources
        calendar: Calendar name
        constraint_type: Constraint type if any
        constraint_date: Constraint date if applicable
    """
    activity_id: str
    activity_code: str
    activity_name: str
    original_duration: float
    remaining_duration: float
    actual_duration: float = 0.0
    start_date: Optional[datetime] = None
    finish_date: Optional[datetime] = None
    actual_start: Optional[datetime] = None
    actual_finish: Optional[datetime] = None
    total_float: float = 0.0
    free_float: float = 0.0
    status: ActivityStatus = ActivityStatus.NOT_STARTED
    percent_complete: float = 0.0
    wbs: str = ""
    predecessors: List[str] = field(default_factory=list)
    successors: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)
    calendar: str = ""
    constraint_type: Optional[str] = None
    constraint_date: Optional[datetime] = None
    is_critical: bool = False
    is_milestone: bool = False
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Determine if activity is milestone
        if self.original_duration == 0:
            self.is_milestone = True
        
        # Determine if activity is critical
        if self.total_float is not None and self.total_float <= 0:
            self.is_critical = True


@dataclass
class Schedule:
    """
    Represents a complete project schedule.
    
    Attributes:
        project_id: Project identifier
        project_name: Project name
        data_date: Schedule data date
        start_date: Project start date
        finish_date: Project finish date
        activities: Dictionary of activities keyed by activity_id
        calendars: Project calendars
        relationships: Activity relationships
        file_path: Path to source XER/XML file
    """
    project_id: str
    project_name: str
    data_date: datetime
    start_date: datetime
    finish_date: datetime
    activities: Dict[str, Activity] = field(default_factory=dict)
    calendars: Dict[str, Any] = field(default_factory=dict)
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    file_path: str = ""
    
    def get_critical_path(self) -> List[Activity]:
        """Return list of critical path activities."""
        return [act for act in self.activities.values() if act.is_critical]
    
    def get_milestones(self) -> List[Activity]:
        """Return list of milestone activities."""
        return [act for act in self.activities.values() if act.is_milestone]


@dataclass
class DelayEvent:
    """
    Represents a delay event identified in analysis.
    
    Attributes:
        activity_id: Activity experiencing delay
        activity_name: Activity name
        delay_days: Number of days delayed
        delay_type: Classification of delay
        start_date: When delay started
        end_date: When delay ended
        cause: Description of delay cause
        responsible_party: Party responsible for delay
        impact_on_project: Days of impact on project completion
        is_concurrent: Whether delay is concurrent with other delays
        window_id: Forensic window identifier
    """
    activity_id: str
    activity_name: str
    delay_days: float
    delay_type: DelayType
    start_date: datetime
    end_date: Optional[datetime]
    cause: str = ""
    responsible_party: str = ""
    impact_on_project: float = 0.0
    is_concurrent: bool = False
    window_id: Optional[str] = None
    notes: str = ""


@dataclass
class ForensicWindow:
    """
    Represents a forensic analysis window.
    
    Attributes:
        window_id: Unique window identifier
        start_date: Window start date
        end_date: Window end date
        baseline_schedule: Baseline schedule for this window
        current_schedule: Current/as-built schedule for this window
        delays: List of delays identified in window
        critical_path_changes: Changes to critical path
        float_changes: Dictionary of float changes by activity
    """
    window_id: str
    start_date: datetime
    end_date: datetime
    baseline_schedule: Optional[Schedule] = None
    current_schedule: Optional[Schedule] = None
    delays: List[DelayEvent] = field(default_factory=list)
    critical_path_changes: Dict[str, Any] = field(default_factory=dict)
    float_changes: Dict[str, float] = field(default_factory=dict)
    
    def get_total_delay(self) -> float:
        """Calculate total delay days in window."""
        return sum(delay.delay_days for delay in self.delays)
    
    def get_critical_delays(self) -> List[DelayEvent]:
        """Return only delays on critical path."""
        return [d for d in self.delays if d.impact_on_project > 0]


@dataclass
class ComparisonResult:
    """
    Results from comparing baseline and current schedules.
    
    Attributes:
        baseline_schedule: Baseline schedule
        current_schedule: Current schedule
        delayed_activities: Activities with delays
        accelerated_activities: Activities completed early
        new_critical_activities: Activities that became critical
        removed_critical_activities: Activities no longer critical
        float_changes: Changes in total float by activity
        milestone_delays: Delays to milestone activities
        overall_delay: Total project delay in days
        spi: Schedule Performance Index
    """
    baseline_schedule: Schedule
    current_schedule: Schedule
    delayed_activities: List[Activity] = field(default_factory=list)
    accelerated_activities: List[Activity] = field(default_factory=list)
    new_critical_activities: List[Activity] = field(default_factory=list)
    removed_critical_activities: List[Activity] = field(default_factory=list)
    float_changes: Dict[str, float] = field(default_factory=dict)
    milestone_delays: Dict[str, float] = field(default_factory=dict)
    overall_delay: float = 0.0
    spi: float = 1.0
    completion_variance: float = 0.0


