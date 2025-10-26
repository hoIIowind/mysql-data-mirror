# DBSyncer - Database Mirror Script

A Python script that performs **one-way incremental database mirroring** from a source MySQL database to a target MySQL database. It is designed to run as a GitHub Actions job at regular intervals, with detailed logging and summary display in the workflow.

## Features

- **One-way mirroring**: Syncs data from source to target database only.
- **Incremental data load**: Only new, updated, or deleted rows are applied; avoids full table reloads.
- **Idempotent operations**: Safe to run multiple times without data duplication.
- **Change tracking**: Target database includes `operation_type` column (`inserted`, `updated`, `deleted`).
- **Soft deletes**: Deleted source records are marked as `deleted` in target, not physically removed.
- **Timezone-aware timestamps**: `last_updated` column in target table is stored in IST.
- **SSL connections**: Connects securely to both source and target databases using `ssl_mode=REQUIRED`.
- **Batch processing**: Handles large tables efficiently.
- **Error handling**: Graceful handling of connection issues, data integrity problems, and foreign key constraints.
- **Comprehensive logging**: Detailed logs for monitoring and troubleshooting.
- **GitHub Actions summary**: Logs are parsed and displayed in a **clean, table-formatted summary** in the Actions UI.
- **Automated execution**: Can be scheduled or manually triggered via GitHub Actions.

## Prerequisites

- MySQL databases (source and target)
- GitHub repository with Actions enabled
- Python 3.11+ (handled by GitHub Actions)
- GitHub Secrets configured with database credentials

## Setup Instructions

### 1. Repository Setup

1. Clone or create a repository with this code.
2. Ensure the following files exist in your repository:
    - `db_mirror.py` - Main script
    - `requirements.txt` - Python dependencies
    - `.github/workflows/db-mirror.yml` - GitHub Actions workflow

### 2. Database Preparation

#### Source Database

- Table must have a primary key.
- Grant **SELECT** permissions to the database user.

#### Target Database

- Script automatically creates the target table if it doesn't exist.
- Grant **CREATE, SELECT, INSERT, UPDATE** permissions to the database user.
- Target table includes additional columns:
    - `operation_type` VARCHAR(10) – Tracks `inserted`, `updated`, or `deleted`.
    - `last_updated` TIMESTAMP – Tracks the last modification time in IST.

### 3. GitHub Secrets Configuration

Navigate to your repository → Settings → Secrets and variables → Actions, and add:

#### Source Database Secrets
- `SOURCE_DB_HOST` – Hostname/IP
- `SOURCE_DB_PORT` – Port
- `SOURCE_DB_USER` – Username
- `SOURCE_DB_PASSWORD` – Password
- `SOURCE_DB_NAME` – Database name

#### Target Database Secrets
- `TARGET_DB_HOST` – Hostname/IP
- `TARGET_DB_PORT` – Port
- `TARGET_DB_USER` – Username
- `TARGET_DB_PASSWORD` – Password
- `TARGET_DB_NAME` – Database name

#### Table Configuration
- `TABLE_NAME` – Name of the table to mirror

### 4. Workflow Activation

The GitHub Actions workflow will:

- Run automatically (schedule configurable in `.yml`).
- Can be manually triggered from the Actions tab.
- Display logs in a **clean, tabular summary** in the workflow summary.
- Optionally upload raw logs as artifacts.

## Usage

### Automatic Execution
Runs automatically via GitHub Actions on schedule or manual trigger.

### Manual Execution
1. Go to repository Actions tab.
2. Select "Database Mirror" workflow.
3. Click "Run workflow".

### Local Testing
```bash
# Set environment variables
export SOURCE_DB_HOST="your-source-host"
export SOURCE_DB_USER="your-source-user"
# ...set all required variables

# Install dependencies
pip install -r requirements.txt

# Run the script
python db_mirror.py
```

## How It Works

1. **Connection**: Establishes SSL connections to source and target databases.
2. **Table Setup**: Creates target table with `operation_type` and `last_updated` columns if missing.
3. **Data Fetch**: Retrieves all source rows and existing target rows.
4. **Comparison**: Compares rows using primary key(s) to detect new, updated, or deleted rows.
5. **Synchronization**:
    - **New rows**: Inserted with `operation_type='inserted'`.
    - **Changed rows**: Updated with `operation_type='updated'` and `last_updated` in IST.
    - **Deleted rows**: Marked `operation_type='deleted'` (soft delete).
6. **Logging**: Writes detailed logs (`db_mirror.log`) and displays a **formatted summary** in GitHub Actions.

## Monitoring

### GitHub Actions Summary
Example output:

```
## Database Mirror Summary

**Source DB:** Connected
**Target DB:** Connected
**Target Table:** Already exists, skipped creation

| Operation | Count |
|-----------|-------|
| Inserted  | 1     |
| Updated   | 0     |
| Deleted   | 0     |

**Status:** Database mirroring completed successfully
```

### Logs
- View logs in Actions run details.
- Local execution creates `db_mirror.log`.

## Security Considerations

- Credentials stored securely as GitHub Secrets.
- Connections use SSL (`ssl_mode=REQUIRED`).
- Only necessary privileges granted (read-only for source recommended).

## Troubleshooting

### Common Issues

1. **Connection Failures**
    - Verify credentials and port.
    - Check network/firewall access from GitHub runners.

2. **Permission Errors**
    - Source DB: SELECT required.
    - Target DB: CREATE, SELECT, INSERT, UPDATE required.

3. **Primary Key Issues**
    - Source table must have primary key.
    - Composite keys are supported.

4. **Foreign Key Constraints**
    - Script handles FK constraints gracefully.
    - If soft delete conflicts occur, check referenced tables.

5. **Large Dataset Performance**
    - Script is optimized for incremental loads.
    - Batch processing reduces memory usage.
    - Consider indexing large tables.