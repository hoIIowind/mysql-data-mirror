# DBSyncer - Database Mirror Script

A Python script that performs one-way database mirroring from a source MySQL database to a target MySQL database, designed to run as a GitHub Actions job at regular intervals.

## Features

- **One-way mirroring**: Syncs data from source to target database only
- **Idempotent operations**: Safe to run multiple times without data duplication
- **Change tracking**: Target database includes `operation_type` column (inserted/updated/deleted)
- **Soft deletes**: Deleted source records are marked as 'deleted' in target, not removed
- **Performance optimized**: Efficient handling of large datasets
- **Error handling**: Graceful handling of connection issues and data integrity problems
- **Comprehensive logging**: Detailed logs for monitoring and troubleshooting
- **Automated execution**: Runs every 5 minutes via GitHub Actions

## Prerequisites

- MySQL databases (source and target)
- GitHub repository with Actions enabled
- Python 3.11+ (handled by GitHub Actions)

## Setup Instructions

### 1. Repository Setup

1. Clone or create a repository with this code
2. Ensure the following files are in your repository:
   - `db_mirror.py` - Main script
   - `requirements.txt` - Python dependencies
   - `.github/workflows/db-mirror.yml` - GitHub Actions workflow

### 2. Database Preparation

#### Source Database
- Ensure the table you want to mirror has a primary key
- Grant SELECT permissions to the database user

#### Target Database
- The script will automatically create the target table if it doesn't exist
- Grant CREATE, SELECT, INSERT, UPDATE permissions to the database user
- The target table will include additional columns:
  - `operation_type` VARCHAR(10) - Tracks 'inserted', 'updated', or 'deleted'
  - `last_updated` TIMESTAMP - Tracks when the record was last modified

### 3. GitHub Secrets Configuration

Navigate to your GitHub repository → Settings → Secrets and variables → Actions, and add the following secrets:

#### Source Database Secrets
- `SOURCE_DB_HOST` - Source database hostname/IP
- `SOURCE_DB_PORT` - Source database port (default: 3306)
- `SOURCE_DB_USER` - Source database username
- `SOURCE_DB_PASSWORD` - Source database password
- `SOURCE_DB_NAME` - Source database name

#### Target Database Secrets
- `TARGET_DB_HOST` - Target database hostname/IP
- `TARGET_DB_PORT` - Target database port (default: 3306)
- `TARGET_DB_USER` - Target database username
- `TARGET_DB_PASSWORD` - Target database password
- `TARGET_DB_NAME` - Target database name

#### Table Configuration
- `TABLE_NAME` - Name of the table to mirror

### 4. Workflow Activation

The GitHub Actions workflow will automatically:
- Run every 5 minutes
- Can be manually triggered from the Actions tab
- Upload logs as artifacts for troubleshooting

## Usage

### Automatic Execution
Once configured, the script runs automatically every 5 minutes via GitHub Actions.

### Manual Execution
1. Go to your repository's Actions tab
2. Select "Database Mirror" workflow
3. Click "Run workflow"

### Local Testing
```bash
# Set environment variables
export SOURCE_DB_HOST="your-source-host"
export SOURCE_DB_USER="your-source-user"
# ... set all required variables

# Install dependencies
pip install -r requirements.txt

# Run the script
python db_mirror.py
```

## How It Works

1. **Connection**: Establishes connections to both source and target databases
2. **Table Setup**: Creates target table with additional tracking columns if needed
3. **Data Fetch**: Retrieves all data from source table and existing target data
4. **Comparison**: Compares source and target data using primary keys
5. **Synchronization**:
   - **New records**: Inserted with `operation_type = 'inserted'`
   - **Changed records**: Updated with `operation_type = 'updated'`
   - **Deleted records**: Marked with `operation_type = 'deleted'` (not physically deleted)
6. **Logging**: Records all operations and any errors

## Monitoring

### Logs
- View logs in GitHub Actions run details
- Logs are uploaded as artifacts and retained for 7 days
- Local execution creates `db_mirror.log` file

### Key Metrics Logged
- Number of records inserted, updated, and deleted
- Connection status and errors
- Execution time and performance metrics

## Security Considerations

- Database credentials are stored as GitHub Secrets (encrypted)
- No credentials are logged or exposed in code
- Connections use secure practices
- Read-only access recommended for source database

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify database credentials in GitHub Secrets
   - Check network connectivity and firewall rules
   - Ensure database servers are accessible from GitHub Actions runners

2. **Permission Errors**
   - Source database user needs SELECT permission
   - Target database user needs CREATE, SELECT, INSERT, UPDATE permissions

3. **Primary Key Issues**
   - Source table must have a primary key
   - Composite primary keys are supported

4. **Large Dataset Performance**
   - Script is optimized for large datasets
   - Consider database indexing for better performance
   - Monitor GitHub Actions execution time limits