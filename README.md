# Accounting SaaS ETL Pipeline

A production-ready ETL pipeline demonstrating PySpark, Apache Iceberg, and modern data engineering practices for accounting systems.

## What This Demonstrates

- **PySpark ETL** with PyMongo extraction from MongoDB
- **Apache Iceberg** tables with ACID transactions and CDC
- **Data partitioning** and optimization strategies
- **Local development** environment with production patterns
- **Query engine** evaluation and recommendations

## Quick Start

```bash
# 1. Setup environment
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Start infrastructure  
docker-compose up -d && sleep 30

# 3. Run ETL pipeline
python run_spark_pipeline.py

# 4. Verify results
python verify_iceberg_results.py
```

## Prerequisites

### 1. Install Java 17 (Required for PySpark)

**For Mac:**
```bash
# Install Java 17 using Homebrew
brew install --cask temurin@17

# Set JAVA_HOME environment variable
echo 'export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc

# Reload shell configuration
source ~/.zshrc

# Verify installation
java -version
# Should show: openjdk version "17.x.x"
```

**For Windows:**
1. Download Java 17 from: https://adoptium.net/temurin/releases/
2. Install the `.msi` file
3. Add to PATH in System Environment Variables
4. Set JAVA_HOME to installation directory

**For Linux (Ubuntu/Debian):**
```bash
# Install Java 17
sudo apt update
sudo apt install openjdk-17-jdk

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

# Verify installation
java -version
```

### 2. Other Prerequisites
- Docker Desktop with 8GB+ memory allocation
- Python 3.9+ with pip
- Git (for cloning repository)

## Installation Guide

### Step 1: Clone Repository
```bash
# Clone the repository
git clone https://github.com/skpachunuri23/floqast.git
cd floqast

# Verify you're in the right directory
pwd
# Should show: /path/to/floqast
```

### Step 2: Set Up Python Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Mac/Linux
# For Windows: venv\Scripts\activate

# Verify activation (should show (venv) in prompt)
which python
```

### Step 3: Install Python Dependencies
```bash
# Install all required packages
pip install -r requirements.txt

# Verify key packages installed
pip list | grep -E "(pyspark|pymongo|boto3)"
```

### Step 4: Start Docker Infrastructure
```bash
# Start MongoDB and MinIO services
docker-compose up -d

# Wait for services to start
sleep 30

# Verify services are running
docker-compose ps
# Should show: accounting-mongodb and accounting-minio running
```

### Step 5: Run the ETL Pipeline
```bash
# Run the full PySpark Iceberg pipeline
python run_spark_pipeline.py

# Expected output should show:
# ✓ Extracted X records from each collection
# ✓ Transformed with flattening and partitioning
# ✓ Created Iceberg tables with optimization
# ✓ Performed MERGE INTO operations for CDC
```

### Step 6: Verify Results
```bash
# Run verification script
python verify_iceberg_results.py

# Check MinIO web interface
open http://localhost:9001
# Login: minioadmin/minioadmin
# Browse: accounting-data-lake bucket
``` 

## What It Does

### Extract
- Uses PyMongo to read from MongoDB collections
- Converts to Spark DataFrames for distributed processing

### Transform  
- Flattens nested journal entry lines into separate rows
- Calculates total debits/credits per journal entry
- Adds year/month partition columns for optimization

### Load
- Creates Iceberg tables with proper partitioning
- Uses MERGE INTO for incremental updates (CDC)
- Implements retention policies and compaction

## Architecture

```
MongoDB → PySpark → Iceberg (S3) → Query Engine
```

**Local Stack:**
- MongoDB: Source accounting data
- MinIO: S3-compatible storage
- PySpark: ETL processing engine
- Iceberg: Modern table format with ACID transactions

## Sample Data

The pipeline processes:
- **6 accounts** (Assets, Liabilities, Equity, Revenue, Expenses)
- **3 journal entries** with nested line items
- **3 close tasks** for month-end processing

## Validating Results

### 1. Check Pipeline Output
```bash
python verify_iceberg_results.py
```

**Expected Output:**
```
📊 Verifying Iceberg ETL Results...
📁 Found 30+ Iceberg files
📊 Iceberg Table Summary:
  accounts: 1 data files, 6 metadata files
  journal_entry_lines: 2 data files, 7 metadata files
  journal_entry_totals: 2 data files, 7 metadata files  
  close_tasks: 1 data files, 6 metadata files
📅 Partitioned Data Found:
  - iceberg-warehouse/accounting/journal_entry_lines/data/year=2025/month=5/
  - iceberg-warehouse/accounting/journal_entry_lines/data/year=2025/month=6/
✅ Iceberg implementation is working correctly!
```

### 2. Explore Data in MinIO Web UI

**Access MinIO Console:**
```bash
open http://localhost:9001
# Login: minioadmin / minioadmin
```

**Navigate to Data:**
1. Click on `accounting-data-lake` bucket
2. Browse to `iceberg-warehouse/accounting/`
3. Explore each table folder:
   - `accounts/` - Chart of accounts data
   - `journal_entry_lines/` - Flattened transaction lines
   - `journal_entry_totals/` - Aggregated totals per entry
   - `close_tasks/` - Month-end tasks

**What to Look For:**
- **Data files**: `.parquet` files in `data/` directories
- **Partitioning**: `year=2025/month=5/` folder structure
- **Metadata**: `.avro` and `.json` files showing Iceberg transaction history

### 3. Examine Source Data in MongoDB

**Connect to MongoDB:**
```bash
docker exec -it accounting-mongodb mongosh -u admin -p password123
```

**Explore Collections:**
```javascript
use accounting_db

// View chart of accounts
db.accounts.find().pretty()

// View journal entries (nested structure)
db.journal_entries.find().pretty()

// View close tasks
db.close_tasks.find().pretty()

exit
```

**Sample Data You'll See:**
```javascript
// Original nested journal entry
{
  "entry_id": "JE1001",
  "date": "2025-05-31",
  "lines": [
    { "account_id": "1000", "debit": 1000, "credit": 0 },
    { "account_id": "4000", "debit": 0, "credit": 1000 }
  ],
  "description": "Cash sales"
}
```

### 4. Download and Inspect Parquet Files

**From MinIO UI:**
1. Navigate to any `.parquet` file
2. Click the download icon
3. Open with tools like:
   - **Python**: `pandas.read_parquet('file.parquet')`
   - **DuckDB**: `SELECT * FROM 'file.parquet'`
   - **Online viewers**: parquet-tools.com

**Sample Transformed Data:**
```
entry_id | account_id | debit | credit | year | month | etl_timestamp
JE1001   | 1000       | 1000  | 0      | 2025 | 5     | 2025-09-09 17:49:59
JE1001   | 4000       | 0     | 1000   | 2025 | 5     | 2025-09-09 17:49:59
```

### 5. Data Quality Validation

**Check Business Rules:**
```bash
# The pipeline automatically validates:
# ✓ All journal entries balanced (debits = credits)
# ✓ No negative amounts in transactions
# ✓ Proper data types and formatting
# ✓ Partition structure for optimization
```

**Manual Validation:**
- **Row count**: Journal entries should be flattened (6 lines from 3 entries)
- **Totals**: Each entry should have matching debit/credit totals
- **Partitioning**: Data split by year/month in directory structure
- **Metadata**: Multiple versions showing CDC operations worked

## Query Engine Recommendations

### Local Development: Trino
- **Why**: Multi-source federation testing, zero cloud costs
- **Limitations**: Memory constraints, no enterprise features

### Production Options

**Snowflake (Recommended for Accounting)**
- Native SQL for finance teams
- Built-in compliance and audit features
- Predictable costs with auto-scaling
- Time travel for regulatory requirements

**Databricks**  
- Choose for real-time processing (>1M transactions/hour)
- Advanced ML and data science capabilities
- Complex event processing requirements

**AWS Athena**
- Choose for cost optimization
- Simple reporting and aggregation queries
- Pay-per-query model

### Decision Framework

- **Compliance-heavy**: Snowflake
- **Real-time + ML**: Databricks  
- **Cost-conscious**: Athena

## Key Implementation Details

### PySpark with Iceberg
```python
# Table creation with optimization
CREATE TABLE iceberg_catalog.accounting.journal_entry_lines
USING ICEBERG 
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728'
)
```

### CDC with MERGE INTO
```sql
MERGE INTO iceberg_catalog.accounting.table AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### Data Quality Validation
- All journal entries balanced (debits = credits)
- No negative amounts
- Proper partitioning for query optimization

## File Structure

```
├── README.md                       # This file
├── requirements.txt                # Python dependencies  
├── docker-compose.yml             # Local infrastructure
├── accounting_etl_pipeline.py     # Main PySpark implementation
├── run_spark_pipeline.py          # Execution script
├── verify_iceberg_results.py      # Results validation
└── init-data/01-init.js           # Sample MongoDB data
```

## Troubleshooting

**Java Issues**: Ensure Java 17 is installed and JAVA_HOME is set
**Docker Memory**: Increase Docker Desktop memory to 8GB+
**Port Conflicts**: Check ports 27017, 9000, 9001 are available

## Technical Requirements Met

- ✅ PySpark script using PyMongo
- ✅ Nested data flattening (journal_entries.lines)
- ✅ Debit/credit total calculations  
- ✅ Year/month partitioning
- ✅ Iceberg tables with Spark connector
- ✅ MERGE INTO for CDC updates
- ✅ Table optimization and compaction
- ✅ Retention policies with snapshots

## Production Considerations

**For Snowflake Production:**
- Role-based access control for financial data
- Automated data governance and lineage
- Integration with BI tools (Tableau, Power BI)

**For Databricks Production:**
- Delta Live Tables for pipeline orchestration
- MLflow for model lifecycle management
- Auto-scaling clusters for cost optimization

**For AWS Production:**
- Glue ETL jobs for data processing
- Athena workgroups for cost control
- QuickSight for business intelligence

---

**Built to demonstrate enterprise data engineering capabilities for senior-level positions in financial technology.**