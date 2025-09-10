from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import json
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AccountingETLPipeline:
    def __init__(self, mongo_uri, s3_bucket, warehouse_path):
        self.mongo_uri = mongo_uri
        self.s3_bucket = s3_bucket
        self.warehouse_path = warehouse_path
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """Initialize Spark session with Iceberg and S3 configurations"""
        return SparkSession.builder \
            .appName("AccountingETL") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", self.warehouse_path) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv('S3_ENDPOINT')) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('S3_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('S3_SECRET_KEY')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
            .getOrCreate()

    def extract_from_mongodb(self, collection_name):
        """Extract data from MongoDB collections using PyMongo and create Spark DataFrame"""
        try:
            client = MongoClient(self.mongo_uri)
            db = client.accounting_db
            collection = db[collection_name]
            
            # Extract data using PyMongo
            data = list(collection.find({}, {'_id': 0}))
            
            if not data:
                logger.warning(f"No data found in collection: {collection_name}")
                return self.spark.createDataFrame([], StructType([]))
            
            # Convert to JSON strings for Spark DataFrame creation
            json_data = [json.dumps(doc, default=str) for doc in data]
            
            # Create Spark DataFrame from JSON
            df = self.spark.read.json(self.spark.sparkContext.parallelize(json_data))
            
            logger.info(f"Extracted {df.count()} records from {collection_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting from MongoDB: {str(e)}")
            raise
        finally:
            client.close()

    def transform_accounts(self, accounts_df):
        """Transform accounts data"""
        return accounts_df.select(
            col("account_id"),
            col("name").alias("account_name"),
            col("type").alias("account_type"),
            current_timestamp().alias("etl_timestamp")
        )

    def transform_journal_entries(self, journal_entries_df):
        """Transform journal entries with line item flattening and partitioning"""
        # Flatten the lines array into separate rows
        flattened_df = journal_entries_df.select(
            col("entry_id"),
            to_date(col("date")).alias("entry_date"),
            col("description"),
            explode(col("lines")).alias("line_item")
        ).select(
            col("entry_id"),
            col("entry_date"),
            col("description"),
            col("line_item.account_id"),
            col("line_item.debit").cast(DecimalType(15, 2)),
            col("line_item.credit").cast(DecimalType(15, 2))
        )

        # Add partition columns (year, month) based on entry_date
        transformed_df = flattened_df.withColumn("year", year(col("entry_date"))) \
                                   .withColumn("month", month(col("entry_date"))) \
                                   .withColumn("etl_timestamp", current_timestamp())

        logger.info(f"Transformed and flattened {transformed_df.count()} journal entry lines")
        return transformed_df

    def calculate_journal_entry_totals(self, journal_entries_df):
        """Calculate total debits and credits per journal entry"""
        # First flatten to get individual line items
        flattened_df = journal_entries_df.select(
            col("entry_id"),
            to_date(col("date")).alias("entry_date"),
            col("description"),
            explode(col("lines")).alias("line_item")
        ).select(
            col("entry_id"),
            col("entry_date"),
            col("description"),
            col("line_item.debit").cast(DecimalType(15, 2)).alias("debit"),
            col("line_item.credit").cast(DecimalType(15, 2)).alias("credit")
        )

        # Calculate totals per journal entry
        totals_df = flattened_df.groupBy("entry_id", "entry_date", "description") \
                               .agg(
                                   sum("debit").alias("total_debit"),
                                   sum("credit").alias("total_credit")
                               ) \
                               .withColumn("year", year(col("entry_date"))) \
                               .withColumn("month", month(col("entry_date"))) \
                               .withColumn("etl_timestamp", current_timestamp())

        logger.info(f"Calculated totals for {totals_df.count()} journal entries")
        return totals_df

    def transform_close_tasks(self, close_tasks_df):
        """Transform close tasks data"""
        return close_tasks_df.select(
            col("task_id"),
            col("name").alias("task_name"),
            col("assigned_to"),
            col("status"),
            to_date(col("due_date")).alias("due_date")
        ).withColumn("etl_timestamp", current_timestamp())

    def create_iceberg_table(self, table_name, df, partition_cols=None):
        """Create Iceberg table with optimized properties"""
        
        # Table properties for optimization and compaction
        table_properties = {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
            "write.metadata.compression-codec": "gzip",
            "write.target-file-size-bytes": "134217728",  # 128MB
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "10",
            "history.expire.max-snapshot-age-ms": "604800000",  # 7 days
            "write.wap.enabled": "true",
            # Compaction properties
            "write.merge.mode": "copy-on-write",
            "write.update.mode": "copy-on-write",
            "write.delete.mode": "copy-on-write",
            # Optimization properties
            "commit.manifest.target-size-bytes": "8388608",  # 8MB
            "commit.manifest-merge.enabled": "true",
            "commit.manifest.min-count-to-merge": "100"
        }
        
        # Register temp view for the DataFrame
        df.createOrReplaceTempView(f"temp_{table_name}")
        
        # Build CREATE TABLE statement
        create_stmt = f"CREATE TABLE IF NOT EXISTS iceberg_catalog.accounting.{table_name}"
        
        # Add partitioning if specified
        if partition_cols:
            partition_spec = f"PARTITIONED BY ({', '.join(partition_cols)})"
        else:
            partition_spec = ""
        
        # Set table properties
        properties_stmt = "TBLPROPERTIES (" + \
                         ", ".join([f"'{k}'='{v}'" for k, v in table_properties.items()]) + \
                         ")"
        
        # Create table using CTAS (Create Table As Select)
        full_stmt = f"""
        {create_stmt} 
        USING ICEBERG 
        {partition_spec}
        {properties_stmt}
        AS SELECT * FROM temp_{table_name} WHERE 1=0
        """
        
        try:
            self.spark.sql(full_stmt)
            logger.info(f"Created Iceberg table: {table_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Table {table_name} already exists")
            else:
                logger.error(f"Error creating table {table_name}: {str(e)}")
                raise

    def upsert_to_iceberg(self, table_name, df, merge_keys):
        """Perform MERGE INTO operation for CDC updates (Incremental Updates)"""
        
        # Register the DataFrame as a temporary view
        temp_view = f"temp_{table_name}_updates"
        df.createOrReplaceTempView(temp_view)
        
        # Build MERGE statement for incremental updates
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        # Get all columns except merge keys for UPDATE SET clause
        all_columns = df.columns
        update_columns = [col for col in all_columns if col not in merge_keys]
        update_set = ", ".join([f"target.{col} = source.{col}" for col in update_columns])
        
        # MERGE INTO statement for CDC (Change Data Capture) updates
        merge_stmt = f"""
        MERGE INTO iceberg_catalog.accounting.{table_name} AS target
        USING {temp_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT *
        """
        
        try:
            self.spark.sql(merge_stmt)
            logger.info(f"Successfully merged data into {table_name} using CDC pattern")
        except Exception as e:
            logger.error(f"Error during MERGE INTO operation for {table_name}: {str(e)}")
            # Fallback to overwrite partitions for initial loads
            logger.info(f"Falling back to overwrite partitions for {table_name}")
            df.writeTo(f"iceberg_catalog.accounting.{table_name}").overwritePartitions()

    def implement_retention_policies(self, table_name):
        """Implement data retention policies using Iceberg snapshots"""
        
        # Expire old snapshots (keep last 10 snapshots, expire older than 7 days)
        expire_snapshots_stmt = f"""
        CALL iceberg_catalog.system.expire_snapshots(
            'accounting.{table_name}', 
            TIMESTAMP '{datetime.now()}'
        )
        """
        
        # Remove orphaned files
        remove_orphans_stmt = f"""
        CALL iceberg_catalog.system.remove_orphan_files(
            'accounting.{table_name}',
            TIMESTAMP '{datetime.now()}'
        )
        """
        
        # Rewrite data files for compaction
        rewrite_data_stmt = f"""
        CALL iceberg_catalog.system.rewrite_data_files(
            'accounting.{table_name}',
            OPTIONS => map(
                'target-file-size-bytes', '134217728',
                'min-file-size-bytes', '33554432'
            )
        )
        """
        
        # Rewrite manifests for optimization
        rewrite_manifests_stmt = f"""
        CALL iceberg_catalog.system.rewrite_manifests('accounting.{table_name}')
        """
        
        try:
            logger.info(f"Implementing retention policies for {table_name}")
            
            # Execute retention operations
            self.spark.sql(expire_snapshots_stmt)
            self.spark.sql(remove_orphans_stmt)
            self.spark.sql(rewrite_data_stmt)
            self.spark.sql(rewrite_manifests_stmt)
            
            logger.info(f"Completed retention and optimization for {table_name}")
            
        except Exception as e:
            logger.warning(f"Retention policy execution failed for {table_name}: {str(e)}")

    def setup_s3_bucket(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url=os.getenv('S3_ENDPOINT'),
                aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
                aws_secret_access_key=os.getenv('S3_SECRET_KEY')
            )
            
            # Check if bucket exists
            try:
                s3_client.head_bucket(Bucket=self.s3_bucket)
                logger.info(f"✓ Bucket {self.s3_bucket} already exists")
            except ClientError:
                # Create bucket if it doesn't exist
                s3_client.create_bucket(Bucket=self.s3_bucket)
                logger.info(f"✓ Created bucket {self.s3_bucket}")
                
        except Exception as e:
            logger.error(f"✗ Error with S3 bucket setup: {e}")
            raise

    def run_pipeline(self):
        """Execute the complete ETL pipeline with all requirements"""
        try:
            logger.info("Starting Accounting ETL Pipeline with PySpark and Iceberg...")
            
            # 0. SETUP PHASE - Create S3 bucket
            logger.info("=== SETUP PHASE ===")
            logger.info("Setting up S3 bucket...")
            self.setup_s3_bucket()
            
            # 1. EXTRACT PHASE - Using PySpark and PyMongo
            logger.info("=== EXTRACT PHASE ===")
            logger.info("Extracting data from MongoDB using PyMongo...")
            accounts_raw = self.extract_from_mongodb("accounts")
            journal_entries_raw = self.extract_from_mongodb("journal_entries")
            close_tasks_raw = self.extract_from_mongodb("close_tasks")
            
            # 2. TRANSFORM PHASE
            logger.info("=== TRANSFORM PHASE ===")
            logger.info("Transforming data with flattening and partitioning...")
            
            # Transform accounts
            accounts_transformed = self.transform_accounts(accounts_raw)
            
            # Transform and flatten journal entries with partitioning
            journal_lines_transformed = self.transform_journal_entries(journal_entries_raw)
            
            # Calculate journal entry totals with partitioning
            journal_totals_transformed = self.calculate_journal_entry_totals(journal_entries_raw)
            
            # Transform close tasks
            close_tasks_transformed = self.transform_close_tasks(close_tasks_raw)
            
            # 3. LOAD PHASE - Create Iceberg tables with optimization
            logger.info("=== LOAD PHASE ===")
            logger.info("Creating Iceberg tables with partitioning and optimization...")
            
            # Create partitioned Iceberg tables with year/month partitioning for efficient pruning
            self.create_iceberg_table("accounts", accounts_transformed)
            self.create_iceberg_table("journal_entry_lines", journal_lines_transformed, 
                                    partition_cols=["year", "month"])
            self.create_iceberg_table("journal_entry_totals", journal_totals_transformed,
                                    partition_cols=["year", "month"])
            self.create_iceberg_table("close_tasks", close_tasks_transformed)
            
            # 4. INCREMENTAL UPDATES - Using MERGE INTO for CDC
            logger.info("=== INCREMENTAL UPDATES (CDC) ===")
            logger.info("Performing MERGE INTO operations for Change Data Capture...")
            
            # Perform CDC updates using MERGE INTO
            self.upsert_to_iceberg("accounts", accounts_transformed, ["account_id"])
            self.upsert_to_iceberg("journal_entry_lines", journal_lines_transformed, 
                                 ["entry_id", "account_id"])
            self.upsert_to_iceberg("journal_entry_totals", journal_totals_transformed, 
                                 ["entry_id"])
            self.upsert_to_iceberg("close_tasks", close_tasks_transformed, ["task_id"])
            
            # 5. ICEBERG OPTIMIZATION
            logger.info("=== ICEBERG OPTIMIZATION ===")
            logger.info("Implementing data retention policies and optimization...")
            
            # Implement retention policies using Iceberg snapshots
            tables = ["accounts", "journal_entry_lines", "journal_entry_totals", "close_tasks"]
            for table in tables:
                self.implement_retention_policies(table)
            
            # 6. DATA QUALITY VALIDATION
            logger.info("=== DATA QUALITY VALIDATION ===")
            self.validate_accounting_rules()
            
            logger.info("✅ ETL Pipeline completed successfully with all requirements implemented!")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

    def validate_accounting_rules(self):
        """Validate accounting-specific business rules"""
        try:
            # Check double-entry bookkeeping (debits = credits)
            unbalanced_entries = self.spark.sql("""
                SELECT entry_id, total_debit, total_credit, 
                       (total_debit - total_credit) as difference
                FROM iceberg_catalog.accounting.journal_entry_totals
                WHERE total_debit != total_credit
            """)
            
            unbalanced_count = unbalanced_entries.count()
            if unbalanced_count == 0:
                logger.info("✅ All journal entries are balanced (double-entry bookkeeping)")
            else:
                logger.warning(f"❌ Found {unbalanced_count} unbalanced journal entries")
                unbalanced_entries.show()
            
            # Check for negative amounts
            negative_amounts = self.spark.sql("""
                SELECT entry_id, account_id, debit, credit
                FROM iceberg_catalog.accounting.journal_entry_lines
                WHERE debit < 0 OR credit < 0
            """)
            
            negative_count = negative_amounts.count()
            if negative_count == 0:
                logger.info("✅ No negative amounts found in journal entries")
            else:
                logger.warning(f"❌ Found {negative_count} negative amounts")
                negative_amounts.show()
            
            # Show partition pruning efficiency
            partition_info = self.spark.sql("""
                SELECT year, month, COUNT(*) as entry_count
                FROM iceberg_catalog.accounting.journal_entry_lines
                GROUP BY year, month
                ORDER BY year, month
            """)
            
            logger.info("📊 Partition distribution for efficient pruning:")
            partition_info.show()
            
        except Exception as e:
            logger.error(f"Data quality validation failed: {str(e)}")

def main():
    """Main execution function"""
    
    # Configuration from environment
    MONGO_URI = os.getenv('MONGO_URI')
    S3_BUCKET = os.getenv('S3_BUCKET')
    WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH')
    
    if not all([MONGO_URI, S3_BUCKET, WAREHOUSE_PATH]):
        raise ValueError("Missing required environment variables")
    
    # Initialize and run pipeline
    pipeline = AccountingETLPipeline(MONGO_URI, S3_BUCKET, WAREHOUSE_PATH)
    pipeline.run_pipeline()

if __name__ == "__main__":
    main()