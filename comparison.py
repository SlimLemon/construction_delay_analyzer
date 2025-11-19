"""
comparison.py

Schedule comparison engine:
- Loads baseline and current schedules via schedule_parser
- Produces a ComparisonResult with activity-level deltas,
  critical path changes, float changes, milestone delays, and SPI.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import schedule_parser
from models import (
    Schedule,
    Activity,
    ComparisonResult,
    ActivityChange,
    DelayEvent,
    DelayType,
)
from utils import compute_spi  # make sure this exists in utils.py


class ScheduleComparator:
    """
    Compares two schedules (baseline vs current) and produces a ComparisonResult.
    """

    def __init__(self, baseline_path: str, current_path: str):
        self.baseline: Schedule = schedule_parser.parse_schedule(baseline_path)
        self.current: Schedule = schedule_parser.parse_schedule(current_path)

        # Quick lookup maps
        self._baseline_by_id: Dict[str, Activity] = {
            str(a.activity_id): a for a in self.baseline.activities
        }
        self._current_by_id: Dict[str, Activity] = {
            str(a.activity_id): a for a in self.current.activities
        }

    def compare(self) -> ComparisonResult:
        """
        Main entry point. Builds and returns a ComparisonResult instance.
        """

        activity_changes: List[ActivityChange] = []
        delayed_events: List[DelayEvent] = []
        delayed_activities: List[Activity] = []
        accelerated_activities: List[Activity] = []
        new_critical: List[Activity] = []
        removed_critical: List[Activity] = []
        milestone_delays: List[DelayEvent] = []

        # 1) Activity-by-activity comparison
        for aid, b_act in self._baseline_by_id.items():
            c_act = self._current_by_id.get(aid)
            if c_act is None:
                # Deleted activity
                change = ActivityChange(
                    activity_id=aid,
                    baseline=b_act,
                    current=None,
                    change_type="DELETED",
                    changes={"status": ("Existing", "Deleted")},
                )
                activity_changes.append(change)
                continue

            # Same activity exists in both
            changes, delay_event = self._compare_activities(b_act, c_act)
            if changes:
                activity_changes.append(
                    ActivityChange(
                        activity_id=aid,
                        baseline=b_act,
                        current=c_act,
                        change_type="MODIFIED",
                        changes=changes,
                    )
                )

            if delay_event:
                delayed_events.append(delay_event)
                if delay_event.delay_days > 0:
                    delayed_activities.append(c_act)
                elif delay_event.delay_days < 0:
                    accelerated_activities.append(c_act)

        # 2) New activities (exist only in current)
        for aid, c_act in self._current_by_id.items():
            if aid not in self._baseline_by_id:
                change = ActivityChange(
                    activity_id=aid,
                    baseline=None,
                    current=c_act,
                    change_type="NEW",
                    changes={"status": ("Not present", "New")},
                )
                activity_changes.append(change)

        # 3) Critical path changes (newly critical vs no longer critical)
        for aid, c_act in self._current_by_id.items():
            b_act = self._baseline_by_id.get(aid)
            if not b_act:
                continue
            if (not b_act.is_critical) and c_act.is_critical:
                new_critical.append(c_act)
            elif b_act.is_critical and (not c_act.is_critical):
                removed_critical.append(c_act)

        # 4) Milestone delays (zero-duration or flagged milestones)
        for aid, b_act in self._baseline_by_id.items():
            c_act = self._current_by_id.get(aid)
            if not c_act:
                continue
            if (b_act.duration == 0) or getattr(b_act, "is_milestone", False):
                ms_delay = self._build_milestone_delay(b_act, c_act)
                if ms_delay and ms_delay.delay_days != 0:
                    milestone_delays.append(ms_delay)

        # 5) Overall delay = project finish movement (days)
        overall_delay = self._date_diff_days(
            self.baseline.finish_date, self.current.finish_date
        )

        # 6) SPI / completion variance (you already have utils for this)
        spi = compute_spi(self.baseline, self.current)
        completion_variance = (
            ((self.current.finish_date - self.baseline.finish_date).days
             / max(1, (self.baseline.finish_date - self.baseline.start_date).days))
            * 100.0
            if (self.baseline.start_date and self.baseline.finish_date
                and self.current.finish_date)
            else 0.0
        )

        # 7) Build ComparisonResult (fields aligned with report_generator)
        result = ComparisonResult(
            baseline_schedule=self.baseline,
            current_schedule=self.current,
            activity_changes=activity_changes,
            delay_events=delayed_events,
            delayed_activities=delayed_activities,
            accelerated_activities=accelerated_activities,
            new_critical_activities=new_critical,
            removed_critical_activities=removed_critical,
            milestone_delays=milestone_delays,
            overall_delay=overall_delay,
            spi=spi,
            completion_variance=completion_variance,
        )

        return result

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _compare_activities(
        self, b: Activity, c: Activity
    ) -> Tuple[Dict[str, Tuple[object, object]], Optional[DelayEvent]]:
        """
        Compare a single activity between baseline and current.
        Returns:
          - dict of changed fields
          - optional DelayEvent if there is schedule delay/acceleration
        """
        changes: Dict[str, Tuple[object, object]] = {}

        def check(attr_name: str):
            bv = getattr(b, attr_name, None)
            cv = getattr(c, attr_name, None)
            if bv != cv:
                changes[attr_name] = (bv, cv)

        # Core fields you care about
        for attr in [
            "start_date",
            "finish_date",
            "duration",
            "remaining_duration",
            "total_float",
            "free_float",
            "percent_complete",
            "is_critical",
            "status",
        ]:
            check(attr)

        # Build delay event if dates moved
        delay_event: Optional[DelayEvent] = None
        start_delta = self._date_diff_days(b.start_date, c.start_date)
        finish_delta = self._date_diff_days(b.finish_date, c.finish_date)

        delay_days = max(start_delta, finish_delta)
        if delay_days != 0:
            delay_type = (
                DelayType.EXCUSABLE_COMPENSABLE
                if delay_days > 0
                else DelayType.ACCELERATION
            )

            delay_event = DelayEvent(
                activity_id=str(c.activity_id),
                activity_code=c.activity_code,
                activity_name=c.activity_name,
                baseline_start=b.start_date,
                baseline_finish=b.finish_date,
                actual_start=c.start_date,
                actual_finish=c.finish_date,
                delay_days=delay_days,
                delay_type=delay_type,
                primary_cause="Unclassified",  # later attribution layer can refine
                is_critical=c.is_critical,
                is_concurrent=False,  # concurrency handled later in analysis_engine
            )

        return changes, delay_event

    def _build_milestone_delay(self, b: Activity, c: Activity) -> Optional[DelayEvent]:
        """Create a DelayEvent for a milestone if its finish moved."""
        delay = self._date_diff_days(b.finish_date, c.finish_date)
        if delay == 0:
            return None

        delay_type = (
            DelayType.EXCUSABLE_COMPENSABLE if delay > 0 else DelayType.ACCELERATION
        )

        return DelayEvent(
            activity_id=str(c.activity_id),
            activity_code=c.activity_code,
            activity_name=c.activity_name,
            baseline_start=b.start_date,
            baseline_finish=b.finish_date,
            actual_start=c.start_date,
            actual_finish=c.finish_date,
            delay_days=delay,
            delay_type=delay_type,
            primary_cause="Unclassified",
            is_critical=c.is_critical,
            is_concurrent=False,
        )

    @staticmethod
    def _date_diff_days(d1: Optional[datetime], d2: Optional[datetime]) -> int:
        if not d1 or not d2:
            return 0
        return (d2 - d1).days


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 3:
        print("Usage: python comparison.py <baseline_xer> <current_xer>")
        sys.exit(1)

    baseline_file, current_file = sys.argv[1], sys.argv[2]
    comparator = ScheduleComparator(baseline_file, current_file)
    comp_result = comparator.compare()

    # Simple debug dump
    print(json.dumps({
        "overall_delay_days": comp_result.overall_delay,
        "spi": comp_result.spi,
        "num_changes": len(comp_result.activity_changes),
        "num_delays": len(comp_result.delay_events),
        "num_new_critical": len(comp_result.new_critical_activities),
        "num_removed_critical": len(comp_result.removed_critical_activities),
    }, default=str, indent=2))