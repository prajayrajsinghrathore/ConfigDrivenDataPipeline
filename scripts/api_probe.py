import sys

try:
    import airflow.sdk  # noqa: F401 - availability probe
except ImportError:
    print("Failed to import airflow.sdk")
    sys.exit(1)

import inspect


def probe_symbol(module_path, symbol):
    try:
        mod = __import__(module_path, fromlist=[symbol])
        obj = getattr(mod, symbol)
        return True, obj, ""
    except Exception as e:
        return False, None, str(e)


print("--- 1. Import Probe ---")
symbols = [
    "CronPartitionTimetable",
    "CronPartitionedTimetable",
    "PartitionedAssetTimetable",
    "PartitionedAtRuntime",
    "RollupMapper",
    "FanOutMapper",
    "FixedKeyMapper",
    "IdentityMapper",
    "DayWindow",
    "WeekWindow",
    "MonthWindow",
    "QuarterWindow",
    "YearWindow",
    "WaitForAll",
    "MinimumCount",
    "StartOfDayMapper",
    "StartOfWeekMapper",
    "StartOfMonthMapper",
    "StartOfQuarterMapper",
    "StartOfYearMapper",
]

found_symbols = {}
for sym in symbols:
    success, obj, err = probe_symbol("airflow.sdk", sym)
    if success:
        print(f"[OK] {sym} exists in airflow.sdk")
        found_symbols[sym] = obj
    else:
        print(f"[FAIL] {sym} missing from airflow.sdk ({err})")
        # Find where it is
        # Doing a simple search is hard in a short script, so we'll leave that to grep if needed, or just let it fail and we manually grep later.

print("\n--- 2. Constructor Probe ---")
for sym, obj in found_symbols.items():
    if inspect.isclass(obj) or inspect.isfunction(obj):
        try:
            sig = inspect.signature(obj)
            print(f"[*] {sym} signature: {sig}")
        except ValueError:
            print(f"[*] {sym} signature: <built-in or cannot be introspected>")

print("\n--- 3. State-store Probe ---")
# Check if context has task_state_store / asset_state_store
print("Checking airflow.sdk.execution_time.context...")
try:
    from airflow.sdk.execution_time.context import NEVER_EXPIRE

    print(f"[OK] NEVER_EXPIRE exists: {NEVER_EXPIRE}")
except ImportError as e:
    print(f"[FAIL] NEVER_EXPIRE missing: {e}")

try:
    from airflow.sdk.execution_time.task_runner import get_current_context  # noqa: F401 - availability probe  # pyright: ignore[reportAttributeAccessIssue]

    print("[OK] get_current_context exists")
except ImportError as e:
    print(f"[FAIL] get_current_context missing: {e}")

try:
    from airflow.sdk.definitions.asset import Asset  # noqa: F401 - availability probe

    print("[OK] Asset exists")
except ImportError as e:
    print(f"[FAIL] Asset missing: {e}")

print("\n--- 4. Config Probe ---")
# We just print the config
try:
    from airflow.configuration import conf

    try:
        max_keys = conf.getint("scheduler", "partition_mapper_max_downstream_keys")
        print(f"[OK] [scheduler] partition_mapper_max_downstream_keys = {max_keys}")
    except Exception as e:
        print(f"[FAIL] [scheduler] partition_mapper_max_downstream_keys error: {e}")

    try:
        timeout = conf.getint("core", "callback_execution_timeout")
        print(f"[OK] [core] callback_execution_timeout = {timeout}")
    except Exception as e:
        print(f"[FAIL] [core] callback_execution_timeout error: {e}")
except Exception as e:
    print(f"Failed to access config: {e}")

print("\n--- 5. FAB Probe ---")
try:
    from airflow.providers.fab.auth_manager.security_manager import constants

    print("[OK] Found FAB constants")
    for name in dir(constants):
        if name.startswith("RESOURCE"):
            print(f"  {name} = {getattr(constants, name)}")
except ImportError as e:
    print(f"[FAIL] FAB constants missing: {e}")
