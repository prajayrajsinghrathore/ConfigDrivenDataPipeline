# File: rlam_airflow_framework/factory/schedule_resolver.py
"""
ScheduleResolver - handles Airflow schedule, timetable, and partition logic.
"""

from typing import Dict, Any, Optional, NamedTuple, Tuple, List
import pendulum
import structlog
from airflow.sdk import (
    AssetAll,
    CronPartitionTimetable,
    PartitionedAssetTimetable,
    PartitionedAtRuntime,
    RollupMapper,
    FanOutMapper,
    FixedKeyMapper,
    IdentityMapper,
    DayWindow,
    WeekWindow,
    MonthWindow,
    QuarterWindow,
    YearWindow,
    WaitForAll,
    MinimumCount,
    StartOfDayMapper,
    StartOfWeekMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfYearMapper,
)
from airflow.sdk.definitions.asset import Asset

log = structlog.get_logger(__name__)


class _ScheduleSettings(NamedTuple):
    """Resolved scheduling settings for a DAG (output of resolve_schedule)."""

    timezone: str
    interval: Any
    start_date: pendulum.DateTime
    end_date: Optional[pendulum.DateTime]
    catchup: bool


class ScheduleResolver:
    """Encapsulates schedule interval parsing, timetable setup, and partitioning rules."""

    @staticmethod
    def resolve_schedule(
        schedule_config: Dict[str, Any],
        dag_id: str,
        timezone: str,
        is_partitioned: bool = False,
    ) -> _ScheduleSettings:
        """Parse schedule config into timezone-aware scheduling settings."""

        default_interval = None if is_partitioned else "@daily"
        interval = schedule_config.get("interval", default_interval)

        catchup = schedule_config.get("catchup", False)
        start_date_str = schedule_config.get("start_date")

        if not start_date_str:
            if catchup:
                raise ValueError(
                    f"Configuration error for '{dag_id}': "
                    "Cannot enable 'catchup: true' without an explicitly configured 'start_date'."
                )
            # Safe default for non-catchup pipelines
            start_date_str = pendulum.today(timezone).subtract(days=1).to_date_string()

        start_date = ScheduleResolver._parse_datetime_with_timezone(
            start_date_str,
            timezone,
            dag_id,
            "start_date",
        )
        end_date = None
        if schedule_config.get("end_date"):
            end_date = ScheduleResolver._parse_datetime_with_timezone(
                schedule_config["end_date"], timezone, dag_id, "end_date"
            )
        return _ScheduleSettings(timezone, interval, start_date, end_date, catchup)

    @staticmethod
    def plan_partitioning(
        config: Dict[str, Any],
        schedule: _ScheduleSettings,
        inlets: List[Asset],
        dag_id: str,
        source_name: str,
        tenant_pool_slots: Optional[int] = None,
    ) -> Tuple[Any, bool, Optional[int]]:
        """Resolve partitioning/incremental config into a concrete schedule."""
        schedule_interval = schedule.interval
        timezone = schedule.timezone

        partition_config = config.get("partition", {})
        is_partitioned = partition_config.get("enabled", False)

        incremental_config = config.get("incremental", {})
        is_incremental = incremental_config.get("enabled", False)

        if is_partitioned and is_incremental:
            raise ValueError(
                f"Configuration error for '{source_name}': "
                f"Combining 'partition' and 'incremental' is not supported as it causes race conditions on the global watermark."
            )

        if not is_partitioned:
            return schedule_interval, False, None

        granularity = partition_config.get("granularity", "day")
        mapper_type = partition_config.get("mapper", "fan_out")
        wait_policy_type = partition_config.get("wait_policy", "wait_for_all")
        min_count = partition_config.get("minimum_count", 1)
        max_fan_out = partition_config.get("max_fan_out", 64)

        # Global limit capping
        from airflow.configuration import conf

        try:
            global_max_keys = conf.getint(
                "scheduler", "partition_mapper_max_downstream_keys", fallback=None
            )
        except Exception:
            global_max_keys = None

        if isinstance(global_max_keys, int) and max_fan_out > global_max_keys:
            log.warning(
                f"max_fan_out {max_fan_out} is bounded by global partition_mapper_max_downstream_keys {global_max_keys}.",
                dag_id=dag_id,
            )
            max_fan_out = global_max_keys

        if tenant_pool_slots is not None and max_fan_out > tenant_pool_slots:
            log.warning(
                f"max_fan_out ({max_fan_out}) significantly exceeds tenant pool "
                f"slots ({tenant_pool_slots}). This will cause severe queuing.",
                dag_id=dag_id,
            )

        window_classes = {
            "day": DayWindow,
            "week": WeekWindow,
            "month": MonthWindow,
            "quarter": QuarterWindow,
            "year": YearWindow,
        }
        mapper_classes = {
            "day": StartOfDayMapper,
            "week": StartOfWeekMapper,
            "month": StartOfMonthMapper,
            "quarter": StartOfQuarterMapper,
            "year": StartOfYearMapper,
        }

        window_cls = window_classes.get(granularity, DayWindow)
        upstream_mapper_cls = mapper_classes.get(granularity, StartOfDayMapper)

        if wait_policy_type == "minimum_count":
            wait_policy = MinimumCount(min_count)
        else:
            wait_policy = WaitForAll()

        if mapper_type == "rollup":
            mapper = RollupMapper(
                upstream_mapper=upstream_mapper_cls(),
                window=window_cls(),
                wait_policy=wait_policy,
                max_downstream_keys=max_fan_out,
            )
        elif mapper_type == "fan_out":
            mapper = FanOutMapper(
                upstream_mapper=upstream_mapper_cls(),
                window=window_cls(),
                max_downstream_keys=max_fan_out,
            )
        elif mapper_type == "fixed_key":
            mapper = FixedKeyMapper(
                downstream_key="fixed_key", max_downstream_keys=max_fan_out
            )
        else:
            mapper = IdentityMapper()

        if partition_config.get("runtime_assigned"):
            schedule_interval = PartitionedAtRuntime()
        else:
            is_time_schedule = schedule_interval is not None

            if is_time_schedule:
                if "mapper" in partition_config or "wait_policy" in partition_config:
                    raise ValueError(
                        f"Configuration error for '{source_name}': "
                        f"Time schedules (interval: '{schedule_interval}') cannot be combined with 'mapper' or 'wait_policy'. Set 'interval: null' to use asset-driven scheduling."
                    )
                cron_map = {
                    "@hourly": "0 * * * *",
                    "@daily": "0 0 * * *",
                    "@weekly": "0 0 * * 0",
                    "@monthly": "0 0 1 * *",
                    "@yearly": "0 0 1 1 *",
                }
                cron_interval_granularity = {
                    "@hourly": "hour",
                    "@daily": "day",
                    "@weekly": "week",
                    "@monthly": "month",
                    "@yearly": "year",
                }
                interval_str = str(schedule_interval)
                if "granularity" in partition_config:
                    implied_granularity = cron_interval_granularity.get(interval_str)
                    if (
                        implied_granularity is not None
                        and implied_granularity != granularity
                    ):
                        raise ValueError(
                            f"Configuration error for '{source_name}': "
                            f"schedule interval '{interval_str}' implies "
                            f"'{implied_granularity}' partitions, but "
                            f"'partition.granularity' is set to '{granularity}'. "
                            f"Align the two (e.g. interval: '@monthly' for "
                            f"granularity: 'month') or remove 'granularity' to "
                            f"accept the schedule's own cadence."
                        )
                cron_str = cron_map.get(interval_str, interval_str)
                schedule_interval = CronPartitionTimetable(cron_str, timezone=timezone)
            else:
                schedule_interval = PartitionedAssetTimetable(
                    assets=inlets[0] if len(inlets) == 1 else AssetAll(*inlets),
                    default_partition_mapper=mapper,
                )

        return schedule_interval, True, max_fan_out

    @staticmethod
    def compute_max_active_runs(
        schedule_config: Dict[str, Any],
        is_partitioned: bool,
        tenant_pool_slots: Optional[int],
        dag_id: str,
    ) -> int:
        if not is_partitioned:
            return 1

        explicit_max_active_runs = schedule_config.get("max_active_runs")
        default_runs = 1

        if explicit_max_active_runs is None:
            return default_runs

        if (
            tenant_pool_slots is not None
            and explicit_max_active_runs > tenant_pool_slots
        ):
            log.warning(
                f"Explicit max_active_runs {explicit_max_active_runs} exceeds tenant pool slots {tenant_pool_slots}. Clamping to {tenant_pool_slots}.",
                dag_id=dag_id,
            )
            return tenant_pool_slots
        return explicit_max_active_runs

    @staticmethod
    def _parse_datetime_with_timezone(
        date_string: str, timezone: str, dag_id: str, field_name: str
    ) -> pendulum.DateTime:
        try:
            dt = pendulum.parse(date_string, tz=timezone)
            if not isinstance(dt, pendulum.DateTime):
                raise ValueError(f"Expected a datetime but got {type(dt).__name__}")
            return dt
        except Exception as e:
            log.error(
                f"Failed to parse {field_name} with timezone",
                dag_id=dag_id,
                field=field_name,
                date_string=date_string,
                timezone=timezone,
                error=str(e),
            )
            raise ValueError(
                f"Invalid {field_name} '{date_string}' for timezone '{timezone}': {e}"
            )
