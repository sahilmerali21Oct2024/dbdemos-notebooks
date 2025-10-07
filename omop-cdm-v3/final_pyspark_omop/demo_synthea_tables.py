#!/usr/bin/env python3
"""
Demo: R CreateSyntheaTables to PySpark Conversion
=================================================

This script demonstrates the conversion of the R CreateSyntheaTables function to PySpark.

Original R Function:
```r
CreateSyntheaTables <- function(connectionDetails, syntheaSchema, syntheaVersion = "2.7.0") {
    # Determine SQL file path based on version
    if (syntheaVersion == "2.7.0")
        sqlFilePath <- "synthea_version/v270"
    # ... other versions
    
    sqlFilename <- paste0(sqlFilePath, "/", "create_synthea_tables.sql")
    
    # Load and translate SQL
    translatedSql <- SqlRender::loadRenderTranslateSql(
        sqlFilename = sqlFilename,
        packageName = "ETLSyntheaBuilder", 
        dbms = connectionDetails$dbms,
        synthea_schema = syntheaSchema
    )
    
    # Execute SQL
    conn <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(conn, translatedSql)
    on.exit(DatabaseConnector::disconnect(conn))
}
```

PySpark Conversion:
- Replaces DatabaseConnector with SparkSession
- Replaces SQL file loading with direct CREATE TABLE statements  
- Replaces executeSql() with spark.sql() calls
- Maintains same OMOP CDM table relationships and data types
"""

from pyspark.sql import SparkSession
from synthea_table_creator import SyntheaTableCreator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def demo_r_to_pyspark_conversion():
    """
    Demonstrate the R to PySpark conversion
    """
    
    # Initialize Spark (equivalent to R connectionDetails)
    spark = SparkSession.builder \
        .appName("SyntheaTableCreatorDemo") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create database (equivalent to R syntheaSchema)
        database_name = "synthea_demo"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        
        logger.info("="*60)
        logger.info("R CreateSyntheaTables to PySpark Conversion Demo")
        logger.info("="*60)
        
        # Demo different Synthea versions
        versions_to_test = ["2.7.0", "3.3.0"]
        
        for version in versions_to_test:
            logger.info(f"\nTesting Synthea version {version}:")
            logger.info("-" * 40)
            
            # Original R call would be:
            # CreateSyntheaTables(connectionDetails, syntheaSchema, syntheaVersion)
            
            # PySpark equivalent:
            table_creator = SyntheaTableCreator(spark, database_name)
            table_creator.create_synthea_tables(synthea_version=version)
            
            # Verify tables were created
            tables = spark.sql("SHOW TABLES").collect()
            synthea_tables = [row.tableName for row in tables if row.tableName.startswith('synthea_')]
            
            logger.info(f"Created {len(synthea_tables)} Synthea tables for version {version}:")
            for table in sorted(synthea_tables):
                logger.info(f"  - {table}")
            
            # Show sample table schema (equivalent to R table structure)
            logger.info(f"\nSample table schema for synthea_patients (version {version}):")
            patients_schema = spark.sql(f"DESCRIBE {database_name}.synthea_patients")
            for row in patients_schema.collect():
                logger.info(f"  {row.col_name:20} {row.data_type:15} {row.comment or ''}")
        
        logger.info("\n" + "="*60)
        logger.info("Conversion completed successfully!")
        logger.info("="*60)
        
        # Show comparison summary
        show_conversion_summary()
        
    except Exception as e:
        logger.error(f"Demo failed: {str(e)}")
        raise
    finally:
        spark.stop()

def show_conversion_summary():
    """Show summary of R to PySpark conversions"""
    
    conversions = [
        {
            "R Component": "connectionDetails",
            "PySpark Equivalent": "SparkSession",
            "Description": "Database connection management"
        },
        {
            "R Component": "SqlRender::loadRenderTranslateSql()",
            "PySpark Equivalent": "_get_synthea_table_definitions()",
            "Description": "SQL template loading and rendering"
        },
        {
            "R Component": "DatabaseConnector::executeSql()",
            "PySpark Equivalent": "spark.sql()",
            "Description": "SQL execution"
        },
        {
            "R Component": "@synthea_schema template",
            "PySpark Equivalent": "f'{database_name}.synthea_' prefix",
            "Description": "Schema/table name templating"
        },
        {
            "R Component": "SQL DDL files in inst/sql/",
            "PySpark Equivalent": "Direct CREATE TABLE statements",
            "Description": "Table creation SQL"
        },
        {
            "R Component": "Version-based file selection",
            "PySpark Equivalent": "Version-based schema branching",
            "Description": "Handling different Synthea versions"
        }
    ]
    
    logger.info("\nR to PySpark Conversion Summary:")
    logger.info("-" * 60)
    for conversion in conversions:
        logger.info(f"R: {conversion['R Component']}")
        logger.info(f"   → PySpark: {conversion['PySpark Equivalent']}")
        logger.info(f"     Purpose: {conversion['Description']}")
        logger.info("")

def show_usage_examples():
    """Show usage examples comparing R and PySpark"""
    
    logger.info("Usage Examples:")
    logger.info("="*50)
    
    logger.info("\n1. Original R Usage:")
    logger.info("""
    library("ETLSyntheaBuilder")
    library("DatabaseConnector")
    
    connectionDetails <- createConnectionDetails(
        dbms = "postgresql",
        server = "localhost/synthea", 
        user = "postgres",
        password = "password"
    )
    
    CreateSyntheaTables(
        connectionDetails, 
        syntheaSchema = "synthea_v270",
        syntheaVersion = "2.7.0"
    )
    """)
    
    logger.info("\n2. PySpark Conversion:")
    logger.info("""
    from pyspark.sql import SparkSession
    from synthea_table_creator import SyntheaTableCreator
    
    spark = SparkSession.builder \\
        .appName("SyntheaETL") \\
        .getOrCreate()
    
    table_creator = SyntheaTableCreator(spark, "synthea_v270")
    table_creator.create_synthea_tables(synthea_version="2.7.0")
    """)
    
    logger.info("\n3. Integrated ETL Usage:")
    logger.info("""
    from synthea_omop_etl import SyntheaOMOPETL
    
    etl = SyntheaOMOPETL(database_name="synthea_v270")
    etl.create_synthea_tables(synthea_version="2.7.0")
    """)

if __name__ == "__main__":
    logger.info("Starting R CreateSyntheaTables to PySpark conversion demo...")
    
    # Show usage examples first
    show_usage_examples()
    
    # Run the actual demo
    demo_r_to_pyspark_conversion()
    
    logger.info("Demo completed successfully!")
