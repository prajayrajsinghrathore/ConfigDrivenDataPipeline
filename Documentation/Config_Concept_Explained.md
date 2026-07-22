
# 1. What is Catchup?
**Catchup** determines whether Airflow should run missed DAG runs when you enable a DAG.

## Example Scenario:
- DAG is scheduled to run daily at 9 AM
- We create the DAG on January 10th with start_date = January 1st
- We have 9 "missed" runs (Jan 1-9)

### With catchup: true (default):
- Airflow will immediately run all 9 missed DAG runs
- Useful for: Historical data backfill, ensuring no data is missed

### With catchup: false:
- Airflow will only run from the next scheduled time (Jan 11th at 9 AM)
- Useful for: Real-time data, when historical runs don't matter

#### In the config
```yaml
schedule:
  interval: "@daily"
  start_date: "2024-01-01"
  catchup: false  # Don't run historical instances
```

**For financial data use case:** Almost always use `catchup: false` because:
- Market data APIs often only have current data
- We don't want to hit rate limits with historical requests
- Historical data usually requires different API endpoints

---

# 2. Schedule Interval Formats
Airflow supports multiple schedule formats:

## Cron Expressions (Most Common)
```yaml
schedule:
  interval: "0 9 * * 1-5"   # 9 AM, Monday-Friday
  interval: "0 */4 * * *"   # Every 4 hours
  interval: "30 18 * * *"   # 6:30 PM daily
  interval: "0 8 1 * *"     # 8 AM on 1st of each month
```
**Cron Format:** minute hour day_of_month month day_of_week
- minute: 0-59
- hour: 0-23 (24-hour format)
- day_of_month: 1-31
- month: 1-12
- day_of_week: 0-6 (0=Sunday, 1=Monday, etc.)

## Airflow Presets (Convenient)
```yaml
schedule:
  interval: "@once"      # Run once when enabled
  interval: "@hourly"    # Every hour (0 * * * *)
  interval: "@daily"     # Daily at midnight (0 0 * * *)
  interval: "@weekly"    # Weekly on Sunday at midnight
  interval: "@monthly"   # Monthly on 1st at midnight
  interval: "@yearly"    # Yearly on Jan 1st at midnight
```

## Time Delta (Python-style)
```yaml
schedule:
  interval: "timedelta(minutes=30)"  # Every 30 minutes
  interval: "timedelta(hours=2)"     # Every 2 hours
```

## Examples for Financial Data:
- Market data during trading hours:  
  `interval: "0 9-16 * * 1-5"`  # Every hour, 9 AM-4 PM, weekdays
- End of day processing:  
  `interval: "0 18 * * 1-5"`    # 6 PM on weekdays
- Monthly reporting:  
  `interval: "0 8 1 * *"`       # 8 AM on 1st of month
- Real-time feeds:  
  `interval: "*/5 * * * *"`     # Every 5 minutes

---

# 3. What Do Tags Do?
Tags are labels that help you organize and filter DAGs in the Airflow UI.

## In Airflow Web UI:
- **Filter DAGs:** Click on a tag to see only DAGs with that tag
- **Group related DAGs:** Visually group similar workflows
- **Search:** Find DAGs by tag name

## Example Tag Usage:
```python
# Reuters config
tags: ["reuters", "market_data", "api", "real_time"]
# Bloomberg config  
tags: ["bloomberg", "fixed_income", "bonds", "daily"]
# BlackRock config
tags: ["blackrock", "positions", "sftp", "daily"]
```
### Tag Strategy Use Case:
- Data source tags: `["reuters", "bloomberg", "blackrock"]`
- Data type tags: `["market_data", "positions", "fixed_income", "trades"]`
- Processing type tags: `["api", "sftp", "real_time", "batch"]`
- Frequency tags: `["daily", "hourly", "weekly", "intraday"]`
- Environment tags: `["dev", "staging", "prod"]`

In Airflow UI, you can:
- Filter: "Show me all 'bloomberg' DAGs"
- Group: See all 'real_time' vs 'batch' processing
- Monitor: Track all 'daily' jobs together

---

# Practical Examples for Financial Data:

## High-Frequency Market Data:
```yaml
schedule:
  interval: "*/15 * * * *"  # Every 15 minutes
  start_date: "2024-01-01"
  catchup: false
  tags: ["market_data", "high_frequency", "reuters"]
```

## End-of-Day Processing:
```yaml
schedule:
  interval: "0 19 * * 1-5"  # 7 PM on weekdays
  start_date: "2024-01-01"
  catchup: false
  tags: ["eod", "positions", "blackrock", "daily"]
```

## Monthly Regulatory Reporting:
```yaml
schedule:
  interval: "0 6 1 * *"     # 6 AM on 1st of month
  start_date: "2024-01-01"
  catchup: false
  tags: ["regulatory", "monthly", "compliance"]
```

## One-Time Data Migration:
```yaml
schedule:
  interval: "@once"         # Run once when enabled
  start_date: "2024-01-01"
  timezone: "UTC"
  catchup: false
  tags: ["migration", "historical", "one_time"]
```

---

# 4. Timezone Configuration & DST Handling

## Overview
**All DAG schedules MUST specify a timezone** to ensure predictable execution times. Airflow 3.1.6 requires timezone-aware datetimes for proper DST (Daylight Saving Time) handling.

## Configuration

### Global Default Timezone
Set in `config/global_settings.yaml`:
```yaml
default_settings:
  default_timezone: "UTC"  # Recommended for production
```

### Pipeline-Specific Timezone
Override in each pipeline config:
```yaml
schedule:
  interval: "@daily"
  start_date: "2024-01-01"
  timezone: "America/New_York"  # Override global default
  catchup: false
```

## Timezone Recommendations

### ✅ Recommended: Use UTC for Production
```yaml
schedule:
  timezone: "UTC"
```

**Benefits:**
- ✅ No DST complexity - execution time never shifts
- ✅ Predictable 24-hour intervals
- ✅ Industry best practice for distributed systems
- ✅ Easier debugging and log correlation

### ⚠️  Use Regional Timezones with Caution
```yaml
schedule:
  timezone: "America/New_York"  # EST/EDT transition
  timezone: "Europe/London"     # GMT/BST transition
  timezone: "Asia/Tokyo"        # No DST (safe)
```

**DST Implications:**
- ⚠️  Execution times shift during spring/fall transitions
- ⚠️  Non-existent times during "spring forward" (2:00 AM → 3:00 AM)
- ⚠️  Ambiguous times during "fall back" (2:00 AM occurs twice)

## DST Behavior: Cron vs Timedelta

### Cron Schedules - Adjust for DST ✓
Cron expressions respect the local timezone and adjust during DST transitions:

```yaml
schedule:
  interval: "@daily"  # or "0 0 * * *"
  timezone: "America/New_York"
```

**Example:** Daily at midnight in New York
- **Winter (EST, UTC-5):** Runs at 5:00 AM UTC
- **Summer (EDT, UTC-4):** Runs at 4:00 AM UTC
- Local time (midnight) stays constant, UTC time shifts

### Timedelta Schedules - Fixed UTC Duration ✗
Timedelta schedules maintain constant UTC intervals regardless of DST:

```yaml
schedule:
  interval: "timedelta(days=1)"  # Exactly 24 hours
  timezone: "America/New_York"
```

**Example:** 24-hour interval starting at midnight
- Always 24 hours apart in UTC
- Local execution time shifts during DST transitions
- On spring forward day: skips from 12:00 AM → 1:00 AM
- On fall back day: runs at 12:00 AM twice (different UTC times)

## Valid Timezone Names (IANA Database)

### North America
```yaml
timezone: "America/New_York"      # US Eastern (EST/EDT)
timezone: "America/Chicago"       # US Central (CST/CDT)
timezone: "America/Denver"        # US Mountain (MST/MDT)
timezone: "America/Los_Angeles"   # US Pacific (PST/PDT)
timezone: "America/Phoenix"       # Arizona (no DST)
```

### Europe
```yaml
timezone: "Europe/London"         # UK (GMT/BST)
timezone: "Europe/Paris"          # France (CET/CEST)
timezone: "Europe/Amsterdam"      # Netherlands (CET/CEST)
timezone: "Europe/Berlin"         # Germany (CET/CEST)
```

### Asia/Pacific
```yaml
timezone: "Asia/Tokyo"            # Japan (no DST)
timezone: "Asia/Shanghai"         # China (no DST)
timezone: "Asia/Singapore"        # Singapore (no DST)
timezone: "Australia/Sydney"      # Australia (AEDT/AEST - southern hemisphere DST)
```

### Other
```yaml
timezone: "UTC"                   # Coordinated Universal Time (recommended)
timezone: "Pacific/Honolulu"      # Hawaii (no DST)
```

## DST Transition Dates 2024-2026

### United States (Second Sunday in March, First Sunday in November)
- **2024:** Spring Forward: March 10 | Fall Back: November 3
- **2025:** Spring Forward: March 9  | Fall Back: November 2
- **2026:** Spring Forward: March 8  | Fall Back: November 1

### Europe (Last Sunday in March, Last Sunday in October)
- **2024:** Spring Forward: March 31  | Fall Back: October 27
- **2025:** Spring Forward: March 30  | Fall Back: October 26
- **2026:** Spring Forward: March 29  | Fall Back: October 25

## Non-UTC Timezone Monitoring

When a pipeline uses a non-UTC timezone, the system logs an INFO-level warning:

```
⚠️  NON-UTC TIMEZONE DETECTED - Pipeline is exposed to Daylight Saving Time transitions
    dag_id: "team_alpha_market_data"
    timezone: "America/New_York"
    dst_impact: "Cron schedules will adjust for DST; timedelta schedules will not"
    recommendation: "Consider using UTC for predictable execution times"
```

This helps teams track which pipelines are exposed to DST complexity.

## Examples

### Financial Market Data (Respect Trading Hours)
```yaml
# Run at 9:30 AM ET (market open), adjusts for DST
schedule:
  interval: "30 9 * * 1-5"  # 9:30 AM, Mon-Fri
  start_date: "2024-01-01"
  timezone: "America/New_York"
  catchup: false
  tags: ["market_data", "trading_hours"]
```

**DST Behavior:**
- Winter: Runs at 2:30 PM UTC (9:30 AM EST)
- Summer: Runs at 1:30 PM UTC (9:30 AM EDT)
- Always aligns with NYSE trading hours

### Global 24/7 Monitoring (UTC)
```yaml
# Run every 4 hours, no DST shifts
schedule:
  interval: "0 */4 * * *"  # Every 4 hours
  start_date: "2024-01-01"
  timezone: "UTC"
  catchup: false
  tags: ["monitoring", "24x7", "global"]
```

**Behavior:**
- Runs at 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
- Never shifts, regardless of DST anywhere
- Predictable for global teams

### End-of-Day Processing (Business Timezone)
```yaml
# Run at 6 PM London time, adjusts for GMT/BST
schedule:
  interval: "0 18 * * 1-5"  # 6 PM, Mon-Fri
  start_date: "2024-01-01"
  timezone: "Europe/London"
  catchup: false
  tags: ["eod", "london", "daily"]
```

**DST Behavior:**
- Winter (GMT, UTC+0): Runs at 6:00 PM UTC
- Summer (BST, UTC+1): Runs at 5:00 PM UTC
- Always 6 PM London local time

### Timedelta Schedule (Fixed Interval)
```yaml
# Run every 24 hours starting at midnight UTC
schedule:
  interval: "timedelta(days=1)"
  start_date: "2024-01-01"
  timezone: "UTC"  # Timezone matters less for timedelta
  catchup: false
  tags: ["daily", "fixed_interval"]
```

**Behavior:**
- Exactly 24 hours between runs
- No DST adjustments
- Predictable for data consistency

## Migration Guide

### Before (Naive Datetimes - ❌ Deprecated)
```yaml
schedule:
  interval: "@daily"
  start_date: "2024-01-01"  # Timezone-naive
  catchup: false
```

### After (Timezone-Aware - ✅ Required)
```yaml
schedule:
  interval: "@daily"
  start_date: "2024-01-01"
  timezone: "UTC"  # Explicit timezone
  catchup: false
```

### Fallback Behavior
If `timezone` is omitted, the pipeline uses `global_settings.default_timezone`:
- **Recommended:** Explicitly set timezone in every pipeline config
- **Fallback:** Uses global default (typically "UTC")
- **Monitoring:** INFO log confirms which timezone source is used

## End Date (Optional)
Stop scheduling after a specific date:

```yaml
schedule:
  interval: "@daily"
  start_date: "2024-01-01"
  end_date: "2024-12-31"  # Stop scheduling after this date
  timezone: "UTC"
  catchup: false
```

**Use Cases:**
- Limited-time data migrations
- Seasonal pipelines
- Deprecated data sources
- Compliance retention deadlines

---

# Practical Examples for Financial Data (Updated):

## High-Frequency Market Data:
```yaml
schedule:
  interval: "*/15 * * * *"  # Every 15 minutes
  start_date: "2024-01-01"
  timezone: "UTC"
  catchup: false
  tags: ["market_data", "high_frequency", "reuters"]
```

## End-of-Day Processing:
```yaml
schedule:
  interval: "0 19 * * 1-5"  # 7 PM on weekdays
  start_date: "2024-01-01"
  timezone: "America/New_York"  # Aligns with US market close
  catchup: false
  tags: ["eod", "positions", "blackrock", "daily"]
```

## Monthly Regulatory Reporting:
```yaml
schedule:
  interval: "0 6 1 * *"     # 6 AM on 1st of month
  start_date: "2024-01-01"
  timezone: "Europe/London"  # UK regulatory timezone
  end_date: "2025-12-31"    # One-year retention
  catchup: false
  tags: ["regulatory", "monthly", "compliance"]
```
