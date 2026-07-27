# Data Sources Configuration

## Table of Contents
- [Introduction](#introduction)
- [Supported Data Sources](#supported-data-sources)
- [REST API Sources](#rest-api-sources)
  - [Basic Configuration](#basic-configuration)
  - [HTTP Methods](#http-methods)
  - [Authentication Methods](#authentication-methods)
  - [Token Management](#token-management)
  - [Request Configuration](#request-configuration)
  - [Response Formats](#response-formats)
- [SFTP Sources](#sftp-sources)
  - [Basic Configuration](#basic-configuration-1)
  - [Authentication](#authentication)
  - [Host Key Verification (SSH) — Onboarding a New SFTP Host](#host-key-verification-ssh--onboarding-a-new-sftp-host)
  - [File Processing Modes](#file-processing-modes)
  - [File Patterns](#file-patterns)
- [Connection Management](#connection-management)
- [Error Handling and Retries](#error-handling-and-retries)
- [Complete Examples](#complete-examples)
- [Troubleshooting](#troubleshooting)

---

## Introduction

The Configuration Driven Pipeline supports multiple data source types, each with secure authentication options and resilient error handling. This guide covers how to configure:

- **REST APIs** with OAuth, Bearer Token, and API Key authentication
- **SFTP servers** with SSH key and password authentication
- **Connection pooling** and retry logic
- **Timeout management** and circuit breakers

---

## Supported Data Sources

```mermaid
graph LR
    A[Data Sources] --> B[REST API]
    A --> C[SFTP]
    A --> D[Databases - Planned]
    
    B --> B1[OAuth 2.0]
    B --> B2[Bearer Token]
    B --> B3[API Key]
    B --> B4[No Auth]
    
    C --> C1[SSH Key]
    C --> C2[Password]
    
    style D fill:#E0E0E0
```

| Source Type | Status | Authentication Options | File Formats |
|------------|--------|----------------------|--------------|
| REST API | ✅ Available | OAuth, Bearer Token, API Key, None | JSON, XML |
| SFTP | ✅ Available | SSH Key, Password | CSV, JSON, Parquet, XML |
| Databases | 🚧 Planned | Connection String, IAM | N/A |

---

## REST API Sources

### Basic Configuration

Minimal configuration for a public REST API:

```yaml
data_source:
  name: public_api_example
  type: rest_api
  connection_id: http_default
  endpoint: https://api.example.com/v1/data
  method: GET
  response_format: json
```

**Flow Diagram:**

```mermaid
sequenceDiagram
    participant Config as Pipeline Config
    participant Fetcher as Data Fetcher
    participant API as REST API
    participant Retry as Retry Logic
    
    Config->>Fetcher: Load API config
    Fetcher->>Fetcher: Create retry session
    Fetcher->>API: HTTP Request
    
    alt Success
        API-->>Fetcher: 200 OK + Data
        Fetcher-->>Config: Return data
    else Transient Error (429, 500, 503)
        API-->>Retry: Error response
        Retry->>Retry: Exponential backoff
        Retry->>API: Retry request
    else Permanent Error (400, 404)
        API-->>Fetcher: Client error
        Fetcher-->>Config: Raise exception
    end
```

### HTTP Methods

Supported HTTP methods with examples:

#### GET Request

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/users
  method: GET
  params:
    limit: 100
    offset: 0
    active: true
```

**Equivalent to:** `GET https://api.example.com/users?limit=100&offset=0&active=true`

#### POST Request

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/search
  method: POST
  request_config:
    body:
      query: "customer data"
      filters:
        status: active
        created_after: "2024-01-01"
```

#### PUT Request

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/resources/123
  method: PUT
  request_config:
    body:
      status: updated
      modified_by: pipeline
```

#### DELETE Request

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/resources/123
  method: DELETE
```

### Authentication Methods

#### 1. OAuth 2.0 (Client Credentials Flow)

```yaml
data_source:
  name: oauth_api_example
  type: rest_api
  endpoint: https://api.example.com/v1/data
  method: GET
  authentication:
    type: oauth
    token_url: https://auth.example.com/oauth/token
    client_id: "{{ conn.oauth_creds.login }}"
    client_secret: "{{ conn.oauth_creds.password }}"
    scope: "read:data"  # Optional
```

**OAuth Flow:**

```mermaid
sequenceDiagram
    participant Pipeline as Data Pipeline
    participant Auth as OAuth Server
    participant API as Resource API
    
    Pipeline->>Auth: POST /oauth/token<br/>(client_id, client_secret, grant_type)
    Auth-->>Pipeline: access_token (expires in 3600s)
    Pipeline->>Pipeline: Cache token
    Pipeline->>API: GET /data<br/>Authorization: Bearer {token}
    API-->>Pipeline: Data response
    
    Note over Pipeline: Token expires
    Pipeline->>Auth: Refresh token
    Auth-->>Pipeline: New access_token
```

**Key Features:**
- ✅ Automatic token refresh before expiration
- ✅ Token caching to minimize auth requests
- ✅ Secure credential storage via Airflow connections

#### 2. Bearer Token (Static Token)

```yaml
data_source:
  name: bearer_token_api
  type: rest_api
  endpoint: https://api.example.com/v1/metrics
  method: GET
  authentication:
    type: bearer_token
    credentials: "{{ conn.api_token.password }}"
```

**Headers Sent:**
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### 3. API Key Authentication

**API Key in Header:**
```yaml
data_source:
  name: api_key_header
  type: rest_api
  endpoint: https://api.example.com/v1/data
  method: GET
  authentication:
    type: api_key
    header_name: X-API-Key
    credentials: "{{ conn.api_credentials.password }}"
```

**API Key in Query Parameter:**
```yaml
data_source:
  name: api_key_query
  type: rest_api
  endpoint: https://api.example.com/v1/data
  method: GET
  authentication:
    type: api_key
    query_param: api_key
    credentials: "{{ conn.api_credentials.password }}"
```

#### 4. No Authentication

For public APIs:

```yaml
data_source:
  name: public_api
  type: rest_api
  endpoint: https://official-joke-api.appspot.com/random_joke
  method: GET
  authentication:
    type: none
```

### Token Management

**Automatic Token Caching:**

```mermaid
flowchart TD
    A[Request Data] --> B{Token Cached?}
    B -->|No| C[Request Token from OAuth]
    C --> D[Cache Token with Expiry]
    D --> E[Use Token]
    B -->|Yes| F{Token Expired?}
    F -->|Yes| C
    F -->|No| E
    E --> G[Make API Request]
```

**Configuration:**
- Tokens are cached **in-memory per DAG run**
- Default expiry: `token.expires_in - 60 seconds` (safety buffer)
- Refresh triggered automatically when token expires

### Request Configuration

Advanced HTTP request options:

```yaml
data_source:
  name: advanced_api
  type: rest_api
  endpoint: https://api.example.com/v1/data
  method: POST
  
  request_config:
    headers:
      Content-Type: application/json
      X-API-Version: "2"
      X-Request-ID: "{{ run_id }}"
    
    body:
      query: "SELECT * FROM data"
      format: json
      compression: gzip
    
    params:
      limit: 1000
      offset: 0
    
    timeout: 60  # Override default timeout
    verify_ssl: true  # SSL certificate verification
```

### Response Formats

#### JSON Response (Default)

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/users
  response_format: json
```

**Sample Response:**
```json
{
  "users": [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
  ]
}
```

**Data Extraction:**
- By default, the entire JSON response is loaded as a DataFrame
- Use transformations to extract nested fields

#### XML Response

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/feed.xml
  response_format: xml
  xml_config:
    root_element: items
    row_element: item
```

**Sample XML:**
```xml
<items>
  <item>
    <id>1</id>
    <title>First Item</title>
  </item>
  <item>
    <id>2</id>
    <title>Second Item</title>
  </item>
</items>
```

---

## SFTP Sources

### Basic Configuration

Connect to an SFTP server and download files:

```yaml
data_source:
  name: sftp_file_source
  type: sftp
  connection_id: sftp_default
  remote_path: /data/exports
  file_pattern: "customer_data_*.csv"
  file_format: csv
  processing_mode: batch
```

**SFTP Connection Flow:**

```mermaid
sequenceDiagram
    participant Config as Pipeline Config
    participant Fetcher as SFTP Fetcher
    participant Server as SFTP Server
    participant SSH as SSH Keys
    
    Config->>Fetcher: Load SFTP config
    Fetcher->>SSH: Load known_hosts
    Fetcher->>Server: Connect (with timeout)
    Server-->>Fetcher: SSH handshake
    Fetcher->>Server: Authenticate (key/password)
    Server-->>Fetcher: Authentication success
    Fetcher->>Server: List files matching pattern
    Server-->>Fetcher: File list
    Fetcher->>Server: Download file(s)
    Server-->>Fetcher: File content
    Fetcher->>Fetcher: Parse file format
    Fetcher-->>Config: Return DataFrame
```

### Authentication

#### SSH Key Authentication (Recommended)

```yaml
data_source:
  name: sftp_ssh_key
  type: sftp
  connection_id: sftp_production
  remote_path: /exports/daily
  file_pattern: "*.csv"
  authentication:
    type: ssh_key
    key_file: "{{ conn.sftp_production.extra.key_file }}"
    passphrase: "{{ conn.sftp_production.password }}"  # Optional
```

**Airflow Connection Setup:**
```bash
# Create SFTP connection with SSH key
airflow connections add sftp_production \
  --conn-type sftp \
  --host sftp.example.com \
  --login sftp_user \
  --port 22 \
  --extra '{"key_file": "/path/to/private_key"}'
```

#### Password Authentication

```yaml
data_source:
  name: sftp_password
  type: sftp
  connection_id: sftp_dev
  remote_path: /data
  file_pattern: "*.json"
  authentication:
    type: password
```

**Airflow Connection:**
```bash
airflow connections add sftp_dev \
  --conn-type sftp \
  --host sftp.example.com \
  --login sftp_user \
  --password "secure-password" \
  --port 22
```

### Host Key Verification (SSH) — Onboarding a New SFTP Host

Before the first SFTP pull from any new host, its SSH host key must be trusted.
The framework verifies the server's host key on every connection to prevent
man-in-the-middle attacks (an attacker who spoofs the host would otherwise
receive the connection's credentials and feed back malicious data).

> ⚠️ **The default mode is `strict`.** A host whose key is not already known is
> **rejected** and the task fails with an `SSHException` — it does **not** connect.
> This is deliberate: a new SFTP host will not work until you complete the
> onboarding steps below. There is no "just connect anyway" default.

#### Verification modes

Set via the `SSH_HOST_KEY_MODE` environment variable on the worker/scheduler:

| Mode | Behavior | Use for |
|------|----------|---------|
| `strict` *(default)* | Unknown host key → **reject and fail**. Only keys present in `known_hosts` are trusted. | **Production** (and any shared environment) |
| `warn` | Unknown host key → log a warning and **connect anyway**. | Local/dev experiments only |
| `auto_add` | Unknown host key → add it to `known_hosts` and connect (trust-on-first-use). | One-off local bootstrapping only — **never production** |

The `known_hosts` file location is controlled by `SSH_KNOWN_HOSTS_FILE`
(default: `~/.ssh/known_hosts`). System host keys are also loaded automatically.

#### Onboarding steps (per new SFTP host)

**1. Capture the host key** and, critically, **verify its fingerprint out of band**
(from the SFTP provider's documentation or a support ticket — do not trust the
key just because `ssh-keyscan` returned it):

```bash
# Fetch the host's public keys
ssh-keyscan -p 22 sftp.example.com > sftp.example.com.knownhosts

# Print fingerprints and compare against the value the provider gave you
ssh-keygen -lf sftp.example.com.knownhosts
```

**2. Add the verified key to the framework's `known_hosts`.** Choose the delivery
mechanism that matches how you run Airflow:

- **Kubernetes / AKS (production):** mount the entries via a `Secret`/`ConfigMap`
  onto the worker and scheduler pods, and point `SSH_KNOWN_HOSTS_FILE` at the
  mount path. Add these to the Helm values so every new host is version-controlled:

  ```yaml
  # helm values (illustrative)
  env:
    - name: SSH_HOST_KEY_MODE
      value: "strict"
    - name: SSH_KNOWN_HOSTS_FILE
      value: "/etc/ssh-known-hosts/known_hosts"
  # ...with the file provided by a mounted Secret containing the verified keys
  ```

- **Docker image bake (also fine for prod):** append the verified entries to the
  image's `known_hosts` during build so they ship with the deployment.

- **Local Docker / dev:** append to your `~/.ssh/known_hosts`:

  ```bash
  cat sftp.example.com.knownhosts >> ~/.ssh/known_hosts
  ```

**3. Deploy, then run the pipeline.** With the key present, `strict` mode connects
normally. If you skipped a host, the task fails fast with:
`Unknown host key for <host>. Add to known_hosts or set SSH_HOST_KEY_MODE=warn` —
that means step 1–2 were not completed for that host.

#### Rotating / changing a host key

If the SFTP provider rotates their host key, connections will start failing under
`strict` (the presented key no longer matches the stored one). Re-run step 1 to
capture and **re-verify** the new key, replace the old entry in `known_hosts`, and
redeploy. Never work around a key-mismatch failure by switching to `warn` in
production — a mismatch is exactly the signal `strict` exists to catch.

> **Dev shortcut:** for throwaway local testing you can set
> `SSH_HOST_KEY_MODE=auto_add` to trust-on-first-use, or `warn` to bypass
> verification entirely. Neither is acceptable in any shared or production
> environment.

### File Processing Modes

#### Single File Mode

Process one file per DAG run:

```yaml
data_source:
  type: sftp
  remote_path: /data/latest
  file_pattern: "latest_data.csv"
  processing_mode: single
```

#### Batch File Mode

Process all matching files in a single DAG run:

```yaml
data_source:
  type: sftp
  remote_path: /data/archive
  file_pattern: "customer_*.csv"
  processing_mode: batch
```

**Behavior:**
- All files matching the pattern are downloaded
- Data is concatenated into a single DataFrame
- Useful for daily batches or archived files

### File Patterns

Use glob patterns to match files:

| Pattern | Matches | Example Files |
|---------|---------|---------------|
| `*.csv` | All CSV files | `data.csv`, `export.csv` |
| `customer_*.json` | Files starting with "customer_" | `customer_2024.json`, `customer_backup.json` |
| `data_202401*.parquet` | Files with date prefix | `data_20240101.parquet`, `data_20240131.parquet` |
| `report_[0-9]*.csv` | Files with numeric suffix | `report_001.csv`, `report_999.csv` |

**Example:**
```yaml
data_source:
  type: sftp
  remote_path: /exports/2024
  file_pattern: "sales_202401[0-9]{2}.csv"  # Matches sales_20240101.csv to sales_20240131.csv
  file_format: csv
```

### File Formats

Supported file formats:

#### CSV

```yaml
data_source:
  type: sftp
  file_pattern: "*.csv"
  file_format: csv
  csv_config:
    delimiter: ","
    encoding: utf-8
    skip_rows: 1  # Skip header row
    columns:
      - id
      - name
      - created_at
```

#### JSON

```yaml
data_source:
  type: sftp
  file_pattern: "*.json"
  file_format: json
  json_config:
    orient: records  # List of objects
```

#### Parquet

```yaml
data_source:
  type: sftp
  file_pattern: "*.parquet"
  file_format: parquet
```

#### XML

```yaml
data_source:
  type: sftp
  file_pattern: "*.xml"
  file_format: xml
  xml_config:
    root_element: data
    row_element: record
```

---

## Connection Management

### Airflow Connections

All data sources use **Airflow Connections** for secure credential storage.

**Creating Connections:**

```bash
# HTTP connection for REST APIs
airflow connections add http_default \
  --conn-type http \
  --host api.example.com \
  --schema https

# SFTP connection
airflow connections add sftp_production \
  --conn-type sftp \
  --host sftp.example.com \
  --login sftp_user \
  --port 22 \
  --extra '{"key_file": "/home/airflow/.ssh/id_rsa"}'
```

### Tenant-Aware Connection Resolution

With multi-tenancy, connections are resolved using **tenant prefixes**:

```mermaid
flowchart LR
    A[Pipeline Config] --> B{Tenant}
    B -->|testing| C[test_http_default]
    B -->|production| D[prod_http_default]
    
    C --> E[Development API]
    D --> F[Production API]
```

**Configuration:**
```yaml
# global_settings.yaml
tenants:
  testing:
    connection_prefix: test_
  production:
    connection_prefix: prod_

# Pipeline config
metadata:
  tenant: testing

data_source:
  connection_id: http_default  # Resolved to: test_http_default
```

---

## Error Handling and Retries

### Automatic Retry Logic

REST API requests automatically retry on transient errors:

```yaml
data_source:
  type: rest_api
  endpoint: https://api.example.com/data
  timeout: 30       # Optional: per-source override of the request timeout (seconds)
  max_retries: 3    # Optional: per-source override of the retry count
```

`timeout` and `max_retries` are read from the `data_source` config by
`HttpFetcher` (`rlam_airflow_framework/data_fetchers/http.py`). When omitted,
they fall back to the `TIMEOUT_HTTP_REQUEST` / `TIMEOUT_HTTP_MAX_RETRIES`
environment variables (see [Centralized Timeout Configuration](#error-handling-and-retries)),
which in turn default to 30s / 3 retries.

> The backoff factor between retries is **not** currently a per-source config
> key — it's controlled only via the `TIMEOUT_HTTP_RETRY_BACKOFF` environment
> variable (default `1.0`). A `retry_backoff:` key under `data_source` has no
> effect today.

**Retry Strategy:**

```mermaid
flowchart TD
    A[HTTP Request] --> B{Response Status}
    B -->|2xx Success| C[Return Data]
    B -->|429 Too Many Requests| D[Retry with Backoff]
    B -->|500 Server Error| D
    B -->|502 Bad Gateway| D
    B -->|503 Service Unavailable| D
    B -->|504 Gateway Timeout| D
    B -->|4xx Client Error| E[Fail Immediately]
    
    D --> F{Max Retries?}
    F -->|No| G[Wait: backoff_factor * 2^retry_count]
    G --> A
    F -->|Yes| H[Raise Exception]
```

**Backoff Calculation:**
- Retry 1: Wait `1.0 * 2^0 = 1` second
- Retry 2: Wait `1.0 * 2^1 = 2` seconds
- Retry 3: Wait `1.0 * 2^2 = 4` seconds

### SFTP Timeout Configuration

```yaml
data_source:
  type: sftp
  timeout: 30  # Optional: per-source override, applied to connect/banner/auth/channel
```

`timeout` is read from the `data_source` config by `SftpFetcher`
(`rlam_airflow_framework/data_fetchers/sftp.py`). When set, it overrides the
connect, banner, and auth timeouts *and* the SFTP channel timeout uniformly.

When `timeout` is **not** set in the config, each phase falls back to its own
environment variable instead of a single shared default:

| Phase | Env var | Default |
|-------|---------|---------|
| SSH connect | `TIMEOUT_SFTP_CONNECT` | 30s |
| SSH banner | `TIMEOUT_SFTP_BANNER` | 30s |
| SSH auth | `TIMEOUT_SFTP_AUTH` | 30s |
| SFTP channel | `TIMEOUT_SFTP_CHANNEL` | 30s |

> There is currently no per-source way to set connect/banner/auth/channel
> timeouts *independently* from pipeline config — only the single `timeout`
> key (applied to all four) or the four environment variables above. A
> nested `sftp_config:` block has no effect today.

---

## Complete Examples

### Example 1: OAuth API with Transformations

```yaml
name: salesforce_contacts
description: Daily sync of Salesforce contacts

metadata:
  tenant: production
  owner: sales-ops

schedule:
  interval: "0 2 * * *"  # Daily at 2 AM
  start_date: "2024-01-01"

data_source:
  name: salesforce_api
  type: rest_api
  connection_id: http_default
  endpoint: https://api.salesforce.com/v1/contacts
  method: GET
  authentication:
    type: oauth
    token_url: https://login.salesforce.com/services/oauth2/token
    client_id: "{{ conn.salesforce_oauth.login }}"
    client_secret: "{{ conn.salesforce_oauth.password }}"
  request_config:
    params:
      limit: 10000
      fields: Id,Name,Email,CreatedDate

transformations:
  - type: add_column
    column_name: extracted_at
    formula: "NOW()"

destination:
  primary:
    type: snowflake
    table: raw.salesforce_contacts
```

### Example 2: SFTP CSV Processing

```yaml
name: daily_customer_import
description: Import customer files from SFTP

metadata:
  tenant: production
  owner: data-engineering

schedule:
  interval: "0 6 * * *"  # Daily at 6 AM
  start_date: "2024-01-01"

data_source:
  name: customer_sftp
  type: sftp
  connection_id: sftp_production
  remote_path: /exports/customers
  file_pattern: "customer_export_*.csv"
  file_format: csv
  processing_mode: batch
  csv_config:
    delimiter: ","
    encoding: utf-8

transformations:
  - type: add_column
    column_name: import_date
    formula: "TODAY()"

data_quality_checks:
  enabled: true
  rules:
    - name: customer_id_not_null
      column: customer_id
      type: not_null

destination:
  primary:
    type: snowflake
    table: staging.customers
  archive:
    type: azure_blob
    container: archive
    path: customers/{{ ds }}
```

---

## Troubleshooting

### Common Issues

#### Issue: SSL Certificate Verification Failed

**Error:**
```
SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]
```

**Solution:**
```yaml
data_source:
  request_config:
    verify_ssl: false  # Only for development!
```

#### Issue: OAuth Token Expired

**Error:**
```
401 Unauthorized: Token has expired
```

**Solution:**
- Ensure `token_url`, `client_id`, and `client_secret` are correct
- Token refresh is automatic - check OAuth server logs

#### Issue: SFTP Connection Timeout

**Error:**
```
TimeoutError: Connection to SFTP server timed out
```

**Solution:**
```yaml
data_source:
  sftp_config:
    connect_timeout: 60  # Increase timeout
```

#### Issue: File Not Found on SFTP

**Error:**
```
FileNotFoundError: No files matching pattern 'data_*.csv'
```

**Solution:**
- Verify `remote_path` is correct
- Check file pattern syntax
- Ensure SFTP user has read permissions

---

## Next Steps

- **[Transformations and Enrichments](04_Transformations_And_Enrichments.md)** - Process data with the Formula Engine
- **[Data Quality](05_Data_Quality_And_Validation.md)** - Validate data quality
- **[Data Sinks](08_Data_Sinks.md)** - Configure destinations

---

**Need Help?** Contact the Data Engineering team or check the [Developer Guide](09_Developer_Guide.md).
