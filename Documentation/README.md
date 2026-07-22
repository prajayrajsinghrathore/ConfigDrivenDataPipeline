# Configuration Driven Data Pipeline - User Documentation

Welcome to the comprehensive user guide for the Configuration Driven Data Pipeline platform!

## 📚 Documentation Structure

This documentation is organized into 9 comprehensive guides:

### Getting Started
- **[01 - Overview](01_Overview.md)** 
  - Platform introduction and architecture
  - Key features and benefits
  - Technology stack
  - Quick start guide

### Configuration
- **[02 - Configuration Schemas](02_Configuration_Schemas.md)**
  - Schema validation and structure
  - Metadata and multi-tenancy
  - Global settings
  - Field reference tables

- **[03 - Data Sources](03_Data_Sources.md)**
  - REST API configuration
  - SFTP setup and authentication
  - Connection management
  - Complete examples

### Data Processing
- **[04 - Transformations and Enrichments](04_Transformations_And_Enrichments.md)**
  - Formula Engine (50+ functions)
  - Column transformations
  - Filters and aggregations
  - Security and performance

- **[05 - Data Quality and Validation](05_Data_Quality_And_Validation.md)**
  - Soda Core integration
  - Validation rules
  - Quarantine management
  - HITL (Human-in-the-Loop) workflows

### Operations
- **[06 - Scheduling and Execution](06_Scheduling_And_Execution.md)**
  - Cron schedules and timezones
  - Backfills and catchup
  - Deadlines and SLAs
  - Retry logic and pools

- **[07 - Kafka Events](07_Kafka_Events.md)**
  - Event architecture
  - Topic naming conventions
  - Event schemas
  - Monitoring and observability

- **[08 - Data Sinks](08_Data_Sinks.md)**
  - Snowflake configuration
  - Azure Blob and Data Lake
  - Multiple destinations
  - XCom and temporary storage

### Development
- **[09 - Developer Guide](09_Developer_Guide.md)**
  - Local development setup
  - Testing strategies
  - DAG bundle deployment
  - Debugging and troubleshooting

## 🚀 Quick Links

### For First-Time Users
1. Start with [Overview](01_Overview.md) to understand the platform
2. Read [Configuration Schemas](02_Configuration_Schemas.md) to learn YAML structure
3. Follow [Data Sources](03_Data_Sources.md) to connect your first source
4. Check [Developer Guide](09_Developer_Guide.md) for local setup

### For Pipeline Developers
- [Transformations](04_Transformations_And_Enrichments.md) - Formula Engine reference
- [Data Quality](05_Data_Quality_And_Validation.md) - Validation rules
- [Scheduling](06_Scheduling_And_Execution.md) - Schedule configuration

### For Operations Teams
- [Kafka Events](07_Kafka_Events.md) - Monitoring and alerting
- [Data Sinks](08_Data_Sinks.md) - Destination configuration
- [Scheduling](06_Scheduling_And_Execution.md) - SLAs and deadlines

## 📊 Diagrams and Visual Aids

Each guide includes:
- ✅ **Mermaid diagrams** for architecture and flow visualization
- ✅ **Tables** for quick reference
- ✅ **Code examples** with syntax highlighting
- ✅ **Configuration samples** for copy-paste

## 🔍 Finding Information

### By Topic
Use the table of contents in each document to jump to specific sections.

### By Use Case
- **Setting up a REST API pipeline**: [Data Sources](03_Data_Sources.md) → REST API section
- **Adding transformations**: [Transformations](04_Transformations_And_Enrichments.md) → Formula Engine
- **Configuring data quality checks**: [Data Quality](05_Data_Quality_And_Validation.md) → Validation Rules
- **Setting up schedules**: [Scheduling](06_Scheduling_And_Execution.md) → Cron Expressions
- **Monitoring pipelines**: [Kafka Events](07_Kafka_Events.md) → Event Types

## 🆘 Getting Help

### Troubleshooting Sections
Each guide includes a **Troubleshooting** section with:
- Common errors and solutions
- Debugging tips
- Best practices

### Contact
- **Data Engineering Team**: Reach out via your team's communication channels
- **GitHub Issues**: Report bugs or request features
- **Internal Slack**: #data-pipeline-support

## 📝 Document Conventions

### Code Blocks
```yaml
# YAML configuration examples
name: example_pipeline
```

```python
# Python code examples
from rlam_airflow_framework import ConfigLoader
```

```bash
# Shell commands
docker-compose up -d
```

### Callouts
- ✅ **DO** - Recommended practices
- ❌ **DON'T** - Practices to avoid
- ⚠️ **WARNING** - Important notes
- 💡 **TIP** - Helpful suggestions

### File References
File paths use Markdown links with line numbers:
- Example: See [config.yaml](../config/global_settings.yaml#L1-L10)

## 🔄 Documentation Updates

This documentation is version-controlled alongside the codebase:
- **Last Updated**: February 4, 2026
- **Platform Version**: 3.x (Airflow 3.1.6)
- **Maintained By**: Data Engineering Team

### Contributing to Documentation
Found an error or want to improve the docs? See [Developer Guide](09_Developer_Guide.md) → Contributing Guidelines.

## 📖 Reading Path

### Beginner Track (2-3 hours)
1. [Overview](01_Overview.md) - 15 min
2. [Configuration Schemas](02_Configuration_Schemas.md) - 30 min
3. [Data Sources](03_Data_Sources.md) - 45 min
4. [Developer Guide](09_Developer_Guide.md) - Setup only - 60 min

### Intermediate Track (4-5 hours)
Complete Beginner Track, then:
5. [Transformations](04_Transformations_And_Enrichments.md) - 60 min
6. [Data Quality](05_Data_Quality_And_Validation.md) - 45 min
7. [Data Sinks](08_Data_Sinks.md) - 45 min

### Advanced Track (8-10 hours)
Complete all guides including:
8. [Scheduling](06_Scheduling_And_Execution.md) - 60 min
9. [Kafka Events](07_Kafka_Events.md) - 90 min

---

## 🎯 Start Here

**New to the platform?** 
→ Begin with [01 - Overview](01_Overview.md)

**Ready to build a pipeline?** 
→ Jump to [03 - Data Sources](03_Data_Sources.md)

**Need to debug an issue?** 
→ Check [09 - Developer Guide](09_Developer_Guide.md)

---

**Questions?** Contact the Data Engineering team or check individual guide troubleshooting sections.

*Happy pipeline building! 🚀*
