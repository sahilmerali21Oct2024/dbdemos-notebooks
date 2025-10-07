#!/usr/bin/env python3
"""
Synthea Data Loader - PySpark Implementation
===========================================

This module contains the PySpark conversion of the R LoadSyntheaTables.r function.
It loads Synthea CSV files into staging tables using PySpark DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class SyntheaDataLoader:
    """
    PySpark implementation of R LoadSyntheaTables function
    
    This class converts the R function that uses data.table::fread() and
    DatabaseConnector::insertTable() to load CSV files. Instead, it uses
    PySpark DataFrame operations and write operations.
    """
    
    def __init__(self, spark: SparkSession, database_name: str):
        """
        Initialize the Synthea data loader
        
        Args:
            spark: SparkSession instance
            database_name: Target database name for Synthea tables
        """
        self.spark = spark
        self.database_name = database_name

    def load_synthea_tables(self, 
                           synthea_file_loc: str,
                           bulk_load: bool = False) -> None:
        """
        Load Synthea CSV files into staging tables equivalent to R LoadSyntheaTables function
        
        This function replaces the R logic that:
        1. Uses list.files() to get CSV files in directory
        2. Uses data.table::fread() to read each CSV file
        3. Applies explicit type conversions for date/string/numeric columns
        4. Uses DatabaseConnector::insertTable() to load data
        
        Instead, it:
        1. Uses Python os.listdir() to get CSV files
        2. Uses spark.read.csv() to read each CSV file
        3. Applies PySpark DataFrame transformations for type conversions
        4. Uses DataFrame.write.saveAsTable() to persist data
        
        Args:
            synthea_file_loc: Path to Synthea CSV files directory
            bulk_load: Whether to use bulk loading optimization (PySpark auto-optimizes)
            
        Raises:
            FileNotFoundError: If synthea_file_loc doesn't exist
            Exception: If CSV loading fails
        """
        logger.info(f"Loading Synthea tables from: {synthea_file_loc}")
        
        # Validate file location (equivalent to R file.exists check)
        if not os.path.exists(synthea_file_loc):
            raise FileNotFoundError(
                f"Synthea File Location specified is invalid: {synthea_file_loc}. "
                "Please provide a valid fully qualified (absolute) path to the directory."
            )
        
        # Get CSV files list (equivalent to R list.files with pattern)
        csv_files = [f for f in os.listdir(synthea_file_loc) if f.endswith('.csv')]
        
        if not csv_files:
            logger.warning(f"No CSV files found in {synthea_file_loc}")
            return
        
        # Process each CSV file (equivalent to R for loop)
        for csv_file in csv_files:
            table_name = csv_file.replace('.csv', '')
            file_path = os.path.join(synthea_file_loc, csv_file)
            
            try:
                logger.info(f"Loading: {csv_file}")
                
                # Read CSV file (equivalent to R data.table::fread)
                df = self._read_synthea_csv(file_path)
                
                # Apply type conversions (equivalent to R explicit casting)
                df = self._convert_synthea_data_types(df)
                
                # Save to table (equivalent to R insertTable)
                self._save_synthea_table(df, table_name, bulk_load)
                
                logger.info(f"Successfully loaded {df.count()} rows into synthea_{table_name}")
                
            except Exception as e:
                logger.error(f"Failed to load {csv_file}: {str(e)}")
                raise
        
        logger.info("All Synthea tables loaded successfully")

    def _read_synthea_csv(self, file_path: str) -> DataFrame:
        """
        Read Synthea CSV file with proper options
        
        Equivalent to R data.table::fread() with specific options:
        - stringsAsFactors = FALSE -> handled by PySpark automatically
        - header = TRUE -> header="true" 
        - sep = "," -> sep=","
        - na.strings = "" -> nullValue=""
        """
        return self.spark.read \\
            .option("header", "true") \\
            .option("inferSchema", "false") \\
            .option("sep", ",") \\
            .option("nullValue", "") \\
            .option("emptyValue", "") \\
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \\
            .option("dateFormat", "yyyy-MM-dd") \\
            .csv(file_path)

    def _convert_synthea_data_types(self, df: DataFrame) -> DataFrame:
        """
        Convert Synthea data types equivalent to R explicit type conversions
        
        This replaces the R logic that checks for specific columns and applies
        as.Date(), as.character(), as.numeric() conversions.
        """
        columns = df.columns
        
        # Date columns - convert to DATE type (equivalent to R as.Date with format)
        date_columns = ['START', 'STOP', 'DATE', 'START_DATE', 'END_DATE', 'BIRTHDATE', 'DEATHDATE']
        for col in date_columns:
            if col in columns:
                df = df.withColumn(col, 
                    when(col(col).isNull() | (col(col) == ""), None)
                    .otherwise(to_date(col(col), 'yyyy-MM-dd')))
        
        # String columns - ensure STRING type (equivalent to R as.character)
        string_columns = ['CODE', 'REASONCODE', 'PHONE']
        for col in string_columns:
            if col in columns:
                df = df.withColumn(col, col(col).cast(StringType()))
        
        # Numeric columns - convert to appropriate numeric type (equivalent to R as.numeric)
        numeric_columns = ['UTILIZATION']
        for col in numeric_columns:
            if col in columns:
                df = df.withColumn(col, 
                    when(col(col).isNull() | (col(col) == ""), None)
                    .otherwise(col(col).cast(DoubleType())))
        
        # Additional common Synthea columns that need type conversion
        cost_columns = [
            'BASE_ENCOUNTER_COST', 'TOTAL_CLAIM_COST', 'PAYER_COVERAGE', 
            'BASE_COST', 'PAYER_COVERAGE', 'TOTALCOST', 'HEALTHCARE_EXPENSES', 
            'HEALTHCARE_COVERAGE', 'REVENUE', 'AMOUNT_COVERED', 'AMOUNT_UNCOVERED'
        ]
        for col in cost_columns:
            if col in columns:
                df = df.withColumn(col, 
                    when(col(col).isNull() | (col(col) == ""), None)
                    .otherwise(col(col).cast(DecimalType(12, 2))))
        
        # Integer columns
        int_columns = ['DISPENSES', 'ENCOUNTERS', 'PROCEDURES', 'INCOME']
        for col in int_columns:
            if col in columns:
                df = df.withColumn(col,
                    when(col(col).isNull() | (col(col) == ""), None)
                    .otherwise(col(col).cast(IntegerType())))
        
        # Coordinate columns
        coord_columns = ['LAT', 'LON']
        for col in coord_columns:
            if col in columns:
                df = df.withColumn(col,
                    when(col(col).isNull() | (col(col) == ""), None)
                    .otherwise(col(col).cast(DecimalType(10, 6))))
        
        return df

    def _save_synthea_table(self, df: DataFrame, table_name: str, bulk_load: bool) -> None:
        """
        Save DataFrame to Synthea table
        
        Equivalent to R DatabaseConnector::insertTable() with options:
        - tableName = paste0(syntheaSchema, ".", table_name)
        - dropTableIfExists = FALSE -> mode("overwrite") 
        - createTable = FALSE -> table should exist from create_synthea_tables()
        - bulkLoad = bulk_load -> PySpark optimizes automatically
        - progressBar = TRUE -> logging provides progress feedback
        """
        target_table = f"{self.database_name}.synthea_{table_name}"
        
        # Write DataFrame to table (overwrite mode replaces table contents)
        df.write \\
          .mode("overwrite") \\
          .option("overwriteSchema", "true") \\
          .saveAsTable(target_table)

    def get_synthea_table_summary(self) -> Dict[str, int]:
        """
        Get summary of loaded Synthea tables
        
        Returns:
            Dictionary mapping table names to row counts
        """
        summary = {}
        
        # Get all Synthea tables
        tables = self.spark.sql("SHOW TABLES").collect()
        synthea_tables = [row.tableName for row in tables if row.tableName.startswith('synthea_')]
        
        for table_name in synthea_tables:
            try:
                count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.{table_name}").collect()[0]['count']
                summary[table_name] = count
            except Exception as e:
                logger.warning(f"Could not get count for {table_name}: {str(e)}")
                summary[table_name] = -1
        
        return summary

    def validate_synthea_data(self) -> Dict[str, List[str]]:
        """
        Validate loaded Synthea data quality
        
        Returns:
            Dictionary of validation results
        """
        validation_results = {
            'warnings': [],
            'errors': [],
            'info': []
        }
        
        try:
            # Check if patients table exists and has data
            patient_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.synthea_patients").collect()[0]['count']
            if patient_count == 0:
                validation_results['errors'].append("No patients found in synthea_patients table")
            else:
                validation_results['info'].append(f"Found {patient_count:,} patients")
            
            # Check for required columns in patients table
            patients_schema = self.spark.sql(f"DESCRIBE {self.database_name}.synthea_patients").collect()
            required_columns = ['id', 'birthdate', 'gender', 'race', 'ethnicity']
            existing_columns = [row.col_name.lower() for row in patients_schema]
            
            for required_col in required_columns:
                if required_col not in existing_columns:
                    validation_results['errors'].append(f"Required column '{required_col}' missing from patients table")
            
            # Check for encounters
            encounter_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.synthea_encounters").collect()[0]['count']
            if encounter_count == 0:
                validation_results['warnings'].append("No encounters found in synthea_encounters table")
            else:
                validation_results['info'].append(f"Found {encounter_count:,} encounters")
            
            # Check for conditions
            condition_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.synthea_conditions").collect()[0]['count']
            if condition_count == 0:
                validation_results['warnings'].append("No conditions found in synthea_conditions table")
            else:
                validation_results['info'].append(f"Found {condition_count:,} conditions")
            
        except Exception as e:
            validation_results['errors'].append(f"Validation failed: {str(e)}")
        
        return validation_results


# Convenience function for direct usage
def load_synthea_tables(spark: SparkSession, 
                       database_name: str,
                       synthea_file_loc: str,
                       bulk_load: bool = False) -> None:
    """
    Convenience function equivalent to R LoadSyntheaTables function
    
    Args:
        spark: SparkSession (equivalent to R connectionDetails)
        database_name: Database name (equivalent to R syntheaSchema) 
        synthea_file_loc: Path to CSV files (equivalent to R syntheaFileLoc parameter)
        bulk_load: Bulk loading flag (equivalent to R bulkLoad parameter)
    """
    loader = SyntheaDataLoader(spark, database_name)
    loader.load_synthea_tables(synthea_file_loc, bulk_load)
