#!/usr/bin/env python3
"""
Setup script to run the PySpark Iceberg ETL pipeline
Handles Spark configuration and JAR downloads automatically
"""

import os
import sys

def setup_spark_environment():
    print("Setting up Spark environment with Iceberg support...")
    
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Compatible packages for Spark 3.5
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367"
    ]
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'
    
    print("Spark environment configured successfully!")
    return True

def run_etl_pipeline():
    try:
        setup_spark_environment()
        
        print("Starting PySpark Iceberg ETL Pipeline...")
        
        from accounting_etl_pipeline import AccountingETLPipeline
        from dotenv import load_dotenv
        
        load_dotenv()
        
        MONGO_URI = os.getenv('MONGO_URI')
        S3_BUCKET = os.getenv('S3_BUCKET')  
        WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH')
        
        if not all([MONGO_URI, S3_BUCKET, WAREHOUSE_PATH]):
            raise ValueError("Missing required environment variables")
        
        pipeline = AccountingETLPipeline(MONGO_URI, S3_BUCKET, WAREHOUSE_PATH)
        pipeline.run_pipeline()
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_etl_pipeline()