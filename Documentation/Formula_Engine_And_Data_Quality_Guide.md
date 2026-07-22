# Config-Driven Data Pipeline - Configuration Guide

This document explains the key configuration concepts for the Config-Driven Data Pipeline framework.

## Table of Contents
1. [Scheduling Configuration](#1-scheduling-configuration)
2. [Formula Engine](#2-formula-engine)
3. [Data Quality with Soda Core](#3-data-quality-with-soda-core)
4. [Custom Transformations](#4-custom-transformations)
5. [Data Lineage](#5-data-lineage)
6. [Practical Examples](#practical-examples-for-financial-data)

---

# 1. Scheduling Configuration

## What is Catchup?
**Catchup** determines whether Airflow should run missed DAG runs when you enable a DAG.

### Example Scenario:
- DAG is scheduled to run daily at 9 AM
- We create the DAG on January 10th with start_date = January 1st
- We have 9 "missed" runs (Jan 1-9)

#### With catchup: true (default):
- Airflow will immediately run all 9 missed DAG runs
- Useful for: Historical data backfill, ensuring no data is missed

#### With catchup: false:
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

## Schedule Interval Formats
Airflow supports multiple schedule formats:

### Cron Expressions (Most Common)
```yaml
schedule:
  interval: "0 9 * * 1-5"   # 9 AM, Monday-Friday
  interval: "0 */4 * * *"   # Every 4 hours
  interval: "30 18 * * *"   # 6:30 PM daily
  interval: "0 8 1 * *"     # 8 AM on 1st of each month
```
**Cron Format:** minute hour day_of_month month day_of_week

### Airflow Presets (Convenient)
```yaml
schedule:
  interval: "@once"      # Run once when enabled
  interval: "@hourly"    # Every hour
  interval: "@daily"     # Daily at midnight
  interval: "@weekly"    # Weekly on Sunday
  interval: "@monthly"   # Monthly on 1st
```

## What Do Tags Do?
Tags are labels that help you organize and filter DAGs in the Airflow UI.

```yaml
tags: ["reuters", "market_data", "api", "real_time"]
```

---

# 2. Formula Engine

The Formula Engine provides a **safe, sandboxed** environment for evaluating expressions in YAML configurations. It replaces the old string-matching approach with a proper expression evaluator.

## Available Functions

### Math Functions
| Function | Description | Example |
|----------|-------------|---------|
| `abs(value)` | Absolute value | `abs(profit_loss)` |
| `round(value, decimals)` | Round to decimals | `round(price, 2)` |
| `min(a, b, ...)` | Minimum value | `min(bid, ask)` |
| `max(a, b, ...)` | Maximum value | `max(high, previous_high)` |
| `pow(base, exp)` | Power/exponent | `pow(1.05, years)` |

### String Functions
| Function | Description | Example |
|----------|-------------|---------|
| `upper(string)` | Convert to uppercase | `upper(ticker)` |
| `lower(string)` | Convert to lowercase | `lower(exchange)` |
| `strip(string)` | Remove whitespace | `strip(name)` |
| `left(string, n)` | First n characters | `left(isin, 2)` |
| `right(string, n)` | Last n characters | `right(cusip, 3)` |
| `substring(s, start, end)` | Extract substring | `substring(sedol, 0, 4)` |
| `concat(str1, str2, ...)` | Concatenate strings | `concat(first, ' ', last)` |
| `replace(s, old, new)` | Replace substring | `replace(name, '-', '_')` |
| `contains(s, sub)` | Check contains | `contains(desc, 'bond')` |
| `length(string)` | String length | `length(description)` |

### Date/Time Functions
| Function | Description | Example |
|----------|-------------|---------|
| `now()` | Current datetime | `now()` |
| `today()` | Current date | `today()` |
| `year(date)` | Extract year | `year(trade_date)` |
| `month(date)` | Extract month | `month(settlement_date)` |
| `day(date)` | Extract day | `day(maturity_date)` |
| `format_date(date, fmt)` | Format as string | `format_date(date, '%Y-%m-%d')` |
| `date_diff(d1, d2, unit)` | Date difference | `date_diff(maturity, today(), 'days')` |

### Null Handling Functions
| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(v1, v2, ...)` | First non-null value | `coalesce(preferred_name, name)` |
| `ifnull(value, default)` | Default if null | `ifnull(price, 0)` |
| `is_null(value)` | Check if null | `is_null(cusip)` |
| `is_not_null(value)` | Check if not null | `is_not_null(isin)` |

### Conditional Functions
| Function | Description | Example |
|----------|-------------|---------|
| `if_else(cond, t, f)` | Conditional | `if_else(amount > 1000000, 'LARGE', 'SMALL')` |
| `case_when(c1, v1, ...)` | Multiple conditions | `case_when(rating >= 90, 'A', rating >= 80, 'B', 'C')` |
| `between(val, low, high)` | Range check | `between(price, 0, 1000)` |
| `in_list(val, list)` | List membership | `in_list(status, ['ACTIVE', 'PENDING'])` |

### Type Conversion Functions
| Function | Description | Example |
|----------|-------------|---------|
| `to_string(value)` | Convert to string | `to_string(account_id)` |
| `to_int(value)` | Convert to integer | `to_int(quantity)` |
| `to_float(value)` | Convert to float | `to_float(price)` |
| `to_date(value, fmt)` | Convert to date | `to_date(date_str, '%Y%m%d')` |

## Formula Examples

```yaml
transformation:
  new_columns:
    # Basic arithmetic
    total_value: "quantity * price"
    profit_margin: "(price - cost) / price * 100"
    
    # String operations
    ticker_upper: "upper(ticker)"
    full_name: "concat(first_name, ' ', last_name)"
    country_code: "left(isin, 2)"
    
    # Conditional logic
    trade_size: "if_else(notional > 1000000, 'LARGE', 'SMALL')"
    risk_level: "case_when(var > 0.05, 'HIGH', var > 0.02, 'MEDIUM', 'LOW')"
    
    # Null handling
    display_name: "coalesce(preferred_name, legal_name, 'UNKNOWN')"
    price_clean: "ifnull(price, 0)"
    
    # Date operations
    processed_at: "now()"
    trade_year: "year(trade_date)"
    days_to_maturity: "date_diff(maturity_date, today(), 'days')"
```

## Legacy Formula Support

The engine automatically converts legacy pandas-style formulas:

| Legacy Syntax | New Syntax |
|--------------|------------|
| `field.str.upper()` | `upper(field)` |
| `field.str.lower()` | `lower(field)` |
| `pd.Timestamp.now()` | `now()` |
| `field.dt.year` | `year(field)` |
| `np.where(c, t, f)` | `if_else(c, t, f)` |

---

# 3. Data Quality with Soda Core

The framework integrates **Soda Core** for enterprise-grade data quality validation. DQ checks run as a separate Airflow task between transformation and loading.

## Configuration Structure

```yaml
validation:
  soda_checks:
    checks:
      # Row count validation
      - type: row_count
        min: 1
      
      # Null/missing value checks
      - type: missing_count
        column: customer_id
        max: 0
      
      # Duplicate detection
      - type: duplicate_count
        column: order_id
        max: 0
      
      # Pattern validation with regex
      - type: invalid_percent
        column: email
        max_percent: 5
        valid_regex: "^[\\w.-]+@[\\w.-]+\\.\\w+$"
      
      # Allowed values
      - type: values_in_set
        column: status
        valid_values: ["PENDING", "APPROVED", "REJECTED"]
      
      # Numeric range
      - type: range
        column: amount
        min: 0
        max: 1000000
      
      # Data freshness
      - type: freshness
        column: updated_at
        max_hours: 24
  
  quality_gates:
    fail_threshold: 0.95      # Fail pipeline if <95% checks pass
    warn_threshold: 0.98      # Warn if <98% checks pass
    quarantine_invalid: true  # Route bad records to quarantine
```

## Check Types Reference

| Check Type | Purpose | Required Fields |
|------------|---------|-----------------|
| `row_count` | Ensure minimum records | `min` |
| `missing_count` | Check for nulls | `column`, `max` |
| `duplicate_count` | Detect duplicates | `column`, `max` |
| `invalid_percent` | Validate patterns | `column`, `max_percent`, `valid_regex` |
| `values_in_set` | Allowed values | `column`, `valid_values` |
| `range` | Numeric bounds | `column`, `min`, `max` |
| `freshness` | Data age | `column`, `max_hours` |

## Quarantine Handling

Invalid records are automatically routed to a quarantine table when `quarantine_invalid: true`:

```yaml
destination:
  primary:
    type: snowflake_table
    table: "ANALYTICS.ORDERS"
  
  quarantine:
    type: snowflake_table
    table: "DQ_AUDIT.QUARANTINE_RECORDS"
```

**Quarantine table schema:**
- `quarantine_id`: Unique identifier
- `source_pipeline`: DAG name
- `source_table`: Original destination
- `failed_checks`: Array of failed check names
- `record_data`: Original record as JSON
- `quarantined_at`: Timestamp
- `reprocessed`: Boolean flag

---

# 4. Custom Transformations

For complex business logic that cannot be expressed in formulas, use **custom Python transforms**.

## Configuration

```yaml
transformation:
  # Formula-based columns (simple logic)
  new_columns:
    total: "quantity * price"
  
  # Custom Python for complex logic
  custom_transform:
    python_file: "transforms/order_enrichment.py"
    function: "apply_business_rules"
    kwargs:
      region: "EMEA"
      apply_discounts: true
```

## Python File Structure

```python
# File: dags/transforms/order_enrichment.py

import pandas as pd

def apply_business_rules(df: pd.DataFrame, region: str = 'DEFAULT', apply_discounts: bool = False) -> pd.DataFrame:
    """
    Apply custom business rules to order data.
    
    Args:
        df: Input DataFrame
        region: Region code for filtering
        apply_discounts: Whether to apply regional discounts
        
    Returns:
        Transformed DataFrame
    """
    # Filter by region
    df = df[df['region'] == region].copy()
    
    # Apply complex discount logic
    if apply_discounts:
        df['discount_rate'] = df.apply(calculate_discount, axis=1)
        df['final_price'] = df['price'] * (1 - df['discount_rate'])
    
    # Add derived fields
    df['processing_timestamp'] = pd.Timestamp.now()
    
    return df

def calculate_discount(row):
    """Complex discount calculation based on multiple factors."""
    if row['customer_tier'] == 'PLATINUM' and row['amount'] > 100000:
        return 0.15
    elif row['customer_tier'] == 'GOLD':
        return 0.10
    else:
        return 0.05
```

---

# 5. Data Lineage

The framework supports **OpenLineage** for data lineage tracking, publishing events to Kafka for consumption by tools like Solidatus, Marquez, or custom solutions.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      AIRFLOW DAG                              │
│  ingest → transform → validate_dq → load                     │
│     │         │           │           │                       │
│     └─────────┴───────────┴───────────┘                       │
│                       │                                       │
│              OpenLineage Events                               │
└──────────────────────────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │  Kafka: openlineage.events    │
        │  (30-day retention)           │
        └───────────────────────────────┘
               │                 │
               ▼                 ▼
        ┌─────────────┐   ┌─────────────┐
        │  Marquez    │   │  Solidatus  │
        │  (local)    │   │ (enterprise)│
        └─────────────┘   └─────────────┘
```

## Starting Marquez (Local Development)

```bash
# Start with lineage profile
docker-compose --profile lineage up -d

# Access Marquez UI
# http://localhost:3001
```

## Kafka Topics

| Topic | Purpose | Retention |
|-------|---------|-----------|
| `openlineage.events` | Lineage metadata | 30 days |
| `pipeline-events` | Pipeline status | 7 days |
| `data-quality` | DQ metrics | 7 days |

---

# Practical Examples for Financial Data

## High-Frequency Market Data Pipeline

```yaml
name: "market_data_realtime"

data_source:
  name: "reuters_fx"
  type: rest_api
  endpoint: "https://api.reuters.com/fx/rates"
  authentication:
    type: api_key

schedule:
  interval: "*/5 * * * *"  # Every 5 minutes
  catchup: false
  tags: ["market_data", "fx", "realtime"]

transformation:
  new_columns:
    mid_price: "(bid + ask) / 2"
    spread: "ask - bid"
    spread_bps: "(ask - bid) / mid_price * 10000"
    captured_at: "now()"

validation:
  soda_checks:
    checks:
      - type: row_count
        min: 1
      - type: missing_count
        column: bid
        max: 0
      - type: range
        column: spread_bps
        min: 0
        max: 100  # Alert if spread > 100bps
  quality_gates:
    fail_threshold: 0.90

destination:
  primary:
    type: snowflake_table
    table: "MARKET_DATA.FX_RATES"
```

## End-of-Day Position Processing

```yaml
name: "eod_positions"

data_source:
  name: "blackrock_positions"
  type: sftp
  connection_id: "sftp_blackrock"
  remote_path: "/outbound/positions/*.csv"

schedule:
  interval: "0 19 * * 1-5"  # 7 PM weekdays
  catchup: false
  tags: ["positions", "eod", "blackrock"]

transformation:
  column_types:
    quantity: float
    market_value: float
  new_columns:
    position_date: "today()"
    weight_pct: "market_value / total_nav * 100"
    risk_flag: "if_else(abs(weight_pct) > 5, 'CONCENTRATED', 'NORMAL')"
  
  custom_transform:
    python_file: "transforms/position_enrichment.py"
    function: "enrich_with_security_master"

validation:
  soda_checks:
    checks:
      - type: row_count
        min: 100
      - type: duplicate_count
        column: security_id
        max: 0
      - type: missing_count
        column: market_value
        max: 0
  quality_gates:
    fail_threshold: 0.99
    quarantine_invalid: true

destination:
  primary:
    type: snowflake_table
    table: "POSITIONS.DAILY_HOLDINGS"
  quarantine:
    type: snowflake_table
    table: "DQ_AUDIT.QUARANTINE_RECORDS"
```

## Monthly Regulatory Report

```yaml
name: "monthly_regulatory"

data_source:
  name: "compliance_extract"
  type: rest_api
  endpoint: "https://internal.api/compliance/monthly"

schedule:
  interval: "0 6 1 * *"  # 6 AM on 1st of month
  catchup: false
  tags: ["regulatory", "compliance", "monthly"]

transformation:
  new_columns:
    report_period: "format_date(today(), '%Y-%m')"
    generated_at: "now()"

validation:
  soda_checks:
    checks:
      - type: row_count
        min: 1
      - type: freshness
        column: last_updated
        max_hours: 48

destination:
  primary:
    type: azure_data_lake
    container: "regulatory-reports"
    path: "monthly/{report_period}/"
  backup:
    type: snowflake_table
    table: "COMPLIANCE.MONTHLY_REPORTS"
```
