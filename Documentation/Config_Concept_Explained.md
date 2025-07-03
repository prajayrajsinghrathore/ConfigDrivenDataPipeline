
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
  catchup: false
  tags: ["migration", "historical", "one_time"]
```
