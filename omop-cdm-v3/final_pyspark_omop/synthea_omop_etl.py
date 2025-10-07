#!/usr/bin/env python3
"""
Synthea OMOP Builder - PySpark Implementation
============================================

This script converts Synthea CSV data to OMOP Common Data Model using PySpark.
It replaces the R-based ETLSyntheaBuilder with equivalent PySpark operations.

Author: Converted from R ETLSyntheaBuilder package
"""

import os
import sys
from typing import Dict, Optional, List
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Import custom modules
from omop_cdm_schemas import get_cdm_table_schemas, get_synthea_table_schemas
from synthea_transformations import SyntheaTransformations
from synthea_table_creator import SyntheaTableCreator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SyntheaOMOPETL:
    """
    Main class for Synthea to OMOP CDM ETL using PySpark
    """
    
    def __init__(self, 
                 app_name: str = "SyntheaOMOPETL",
                 warehouse_dir: Optional[str] = None,
                 database_name: str = "omop_cdm"):
        """
        Initialize the ETL processor
        
        Args:
            app_name: Spark application name
            warehouse_dir: Spark warehouse directory (optional)
            database_name: Database name for OMOP CDM tables
        """
        self.app_name = app_name
        self.database_name = database_name
        
        # Build SparkSession
        builder = SparkSession.builder.appName(app_name)
        
        if warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
            
        # Add common configurations
        builder = builder.config("spark.sql.adaptive.enabled", "true") \
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Create database if it doesn't exist
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        self.spark.sql(f"USE {database_name}")
        
        logger.info(f"Initialized Spark session with app name: {app_name}")
        logger.info(f"Using database: {database_name}")

    def create_cdm_tables(self, cdm_version: str = "5.4") -> None:
        """
        Create OMOP CDM tables equivalent to CreateCDMTables.r
        
        Args:
            cdm_version: CDM version ("5.3" or "5.4")
        """
        logger.info(f"Creating CDM v{cdm_version} tables...")
        
        if cdm_version not in ["5.3", "5.4"]:
            raise ValueError("Unsupported CDM version. Supported versions are '5.3' and '5.4'")
        
        # Define OMOP CDM table schemas based on version
        cdm_schemas = self._get_cdm_table_schemas(cdm_version)
        
        for table_name, schema in cdm_schemas.items():
            try:
                # Create empty DataFrame with schema
                empty_df = self.spark.createDataFrame([], schema)
                empty_df.write.mode("overwrite").saveAsTable(f"{self.database_name}.{table_name}")
                logger.info(f"Created table: {table_name}")
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {str(e)}")
                raise
        
        logger.info("CDM tables created successfully")

    def create_synthea_tables(self, synthea_version: str = "2.7.0") -> None:
        """
        Create Synthea staging tables equivalent to CreateSyntheaTables.r
        
        This function is the direct PySpark conversion of the R CreateSyntheaTables function.
        It replaces the R logic of loading SQL files and executing them with direct
        spark.sql() calls to create the Synthea staging tables.
        
        Args:
            synthea_version: Synthea version ("2.7.0", "3.0.0", "3.1.0", "3.2.0", "3.3.0")
        """
        # Delegate to the specialized table creator class
        table_creator = SyntheaTableCreator(self.spark, self.database_name)
        table_creator.create_synthea_tables(synthea_version)

    def load_synthea_tables(self, synthea_file_loc: str) -> None:
        """
        Load Synthea CSV files into staging tables equivalent to LoadSyntheaTables.r
        
        Args:
            synthea_file_loc: Path to Synthea CSV files
        """
        logger.info(f"Loading Synthea tables from: {synthea_file_loc}")
        
        if not os.path.exists(synthea_file_loc):
            raise FileNotFoundError(f"Synthea file location does not exist: {synthea_file_loc}")
        
        # Get all CSV files in the directory
        csv_files = [f for f in os.listdir(synthea_file_loc) if f.endswith('.csv')]
        
        for csv_file in csv_files:
            table_name = csv_file.replace('.csv', '')
            file_path = os.path.join(synthea_file_loc, csv_file)
            
            try:
                logger.info(f"Loading: {csv_file}")
                
                # Read CSV with proper schema inference
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("timestampFormat", "yyyy-MM-dd") \
                    .option("dateFormat", "yyyy-MM-dd") \
                    .csv(file_path)
                
                # Apply data type conversions similar to R code
                df = self._convert_synthea_data_types(df)
                
                # Save as table
                df.write.mode("overwrite").saveAsTable(f"{self.database_name}.synthea_{table_name}")
                
                logger.info(f"Loaded {df.count()} rows into synthea_{table_name}")
                
            except Exception as e:
                logger.error(f"Failed to load {csv_file}: {str(e)}")
                raise
        
        logger.info("Synthea tables loaded successfully")

    def load_vocab_from_csv(self, vocab_file_loc: str) -> None:
        """
        Load vocabulary CSV files equivalent to LoadVocabFromCsv.r
        
        Args:
            vocab_file_loc: Path to vocabulary CSV files
        """
        logger.info(f"Loading vocabulary tables from: {vocab_file_loc}")
        
        if not os.path.exists(vocab_file_loc):
            raise FileNotFoundError(f"Vocabulary file location does not exist: {vocab_file_loc}")
        
        # Standard OMOP vocabulary table names
        vocab_tables = [
            'CONCEPT', 'VOCABULARY', 'DOMAIN', 'CONCEPT_CLASS', 
            'CONCEPT_RELATIONSHIP', 'RELATIONSHIP', 'CONCEPT_SYNONYM',
            'CONCEPT_ANCESTOR', 'SOURCE_TO_CONCEPT_MAP', 'DRUG_STRENGTH'
        ]
        
        for table_name in vocab_tables:
            file_path = os.path.join(vocab_file_loc, f"{table_name}.csv")
            
            if os.path.exists(file_path):
                try:
                    logger.info(f"Loading vocabulary table: {table_name}")
                    
                    df = self.spark.read \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .option("sep", "\t") \
                        .csv(file_path)
                    
                    # Apply vocabulary-specific transformations
                    df = self._convert_vocab_data_types(df, table_name)
                    
                    df.write.mode("overwrite").saveAsTable(f"{self.database_name}.{table_name.lower()}")
                    
                    logger.info(f"Loaded {df.count()} rows into {table_name.lower()}")
                    
                except Exception as e:
                    logger.error(f"Failed to load vocabulary table {table_name}: {str(e)}")
                    raise
            else:
                logger.warning(f"Vocabulary file not found: {file_path}")
        
        logger.info("Vocabulary tables loaded successfully")

    def create_map_and_rollup_tables(self, cdm_version: str = "5.4", synthea_version: str = "2.7.0") -> None:
        """
        Create mapping and rollup tables equivalent to CreateMapAndRollupTables.r
        
        Args:
            cdm_version: CDM version
            synthea_version: Synthea version
        """
        logger.info("Creating vocabulary mapping and visit rollup tables...")
        
        # Create vocabulary mapping tables
        self._create_vocab_map_tables(cdm_version)
        
        # Create visit rollup tables
        self._create_visit_rollup_tables(cdm_version, synthea_version)
        
        logger.info("Mapping and rollup tables created successfully")

    def create_extra_indices(self, synthea_version: str = "2.7.0") -> None:
        """
        Create extra indices for performance equivalent to CreateExtraIndices.r
        
        Args:
            synthea_version: Synthea version
        """
        logger.info("Creating extra indices...")
        
        # Define indices to create
        indices = [
            ("synthea_patients", "id"),
            ("synthea_encounters", "id"),
            ("synthea_encounters", "patient"),
            ("synthea_conditions", "patient"),
            ("synthea_conditions", "encounter"),
            ("synthea_medications", "patient"),
            ("synthea_medications", "encounter"),
            ("synthea_observations", "patient"),
            ("synthea_observations", "encounter"),
            ("synthea_procedures", "patient"),
            ("synthea_procedures", "encounter"),
        ]
        
        # Note: Spark SQL doesn't support explicit index creation like traditional RDBMS
        # Instead, we can cache frequently accessed tables and use partitioning
        for table_name, column in indices:
            try:
                # Cache table for better performance
                self.spark.sql(f"CACHE TABLE {self.database_name}.{table_name}")
                logger.info(f"Cached table: {table_name}")
            except Exception as e:
                logger.warning(f"Could not cache table {table_name}: {str(e)}")
        
        logger.info("Extra indices/caching completed")

    def load_event_tables(self, 
                         cdm_version: str = "5.4", 
                         synthea_version: str = "2.7.0",
                         cdm_source_name: str = "Synthea synthetic health database",
                         cdm_source_abbreviation: str = "Synthea",
                         cdm_holder: str = "OHDSI",
                         cdm_source_description: str = "SyntheaTM is a Synthetic Patient Population Simulator.") -> None:
        """
        Load event tables with OMOP transformations equivalent to LoadEventTables.r
        
        Args:
            cdm_version: CDM version
            synthea_version: Synthea version
            cdm_source_name: Source name for CDM_SOURCE table
            cdm_source_abbreviation: Source abbreviation
            cdm_holder: CDM holder
            cdm_source_description: Source description
        """
        logger.info("Loading OMOP CDM event tables...")
        
        # Define the sequence of table transformations
        etl_steps = [
            ("location", self._transform_location),
            ("care_site", self._transform_care_site),
            ("person", self._transform_person),
            ("observation_period", self._transform_observation_period),
            ("provider", self._transform_provider),
            ("visit_occurrence", self._transform_visit_occurrence),
            ("visit_detail", self._transform_visit_detail),
            ("condition_occurrence", lambda: self._transform_condition_occurrence(synthea_version)),
            ("observation", self._transform_observation),
            ("measurement", lambda: self._transform_measurement(synthea_version)),
            ("procedure_occurrence", lambda: self._transform_procedure_occurrence(synthea_version)),
            ("drug_exposure", self._transform_drug_exposure),
            ("condition_era", self._transform_condition_era),
            ("drug_era", self._transform_drug_era),
            ("cdm_source", lambda: self._transform_cdm_source(cdm_version, cdm_source_name, 
                                                            cdm_source_abbreviation, cdm_holder, 
                                                            cdm_source_description, synthea_version)),
            ("device_exposure", self._transform_device_exposure),
            ("death", self._transform_death),
            ("payer_plan_period", lambda: self._transform_payer_plan_period(synthea_version)),
            ("cost", lambda: self._transform_cost(synthea_version)),
        ]
        
        # Execute each ETL step
        for table_name, transform_func in etl_steps:
            try:
                logger.info(f"Transforming: {table_name}")
                transform_func()
                logger.info(f"Completed: {table_name}")
            except Exception as e:
                logger.error(f"Failed to transform {table_name}: {str(e)}")
                raise
        
        logger.info("Event tables loaded successfully")

    def run_full_etl(self,
                    cdm_version: str = "5.4",
                    synthea_schema: str = "synthea_v270", 
                    synthea_file_loc: str = None,
                    vocab_file_loc: str = None,
                    synthea_version: str = "2.7.0") -> None:
        """
        Run the complete ETL process equivalent to the R codeToRun.R script
        
        Args:
            cdm_version: CDM version
            synthea_schema: Synthea schema name (for compatibility)
            synthea_file_loc: Path to Synthea CSV files
            vocab_file_loc: Path to vocabulary CSV files
            synthea_version: Synthea version
        """
        logger.info("Starting full Synthea to OMOP ETL process...")
        
        try:
            # Step 1: Create CDM tables
            self.create_cdm_tables(cdm_version)
            
            # Step 2: Create Synthea tables
            self.create_synthea_tables(synthea_version)
            
            # Step 3: Load Synthea data
            if synthea_file_loc:
                self.load_synthea_tables(synthea_file_loc)
            
            # Step 4: Load vocabulary data
            if vocab_file_loc:
                self.load_vocab_from_csv(vocab_file_loc)
            
            # Step 5: Create mapping and rollup tables
            self.create_map_and_rollup_tables(cdm_version, synthea_version)
            
            # Step 6: Create extra indices for performance
            self.create_extra_indices(synthea_version)
            
            # Step 7: Load event tables with transformations
            self.load_event_tables(cdm_version, synthea_version)
            
            logger.info("Full ETL process completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

    def _get_cdm_table_schemas(self, cdm_version: str) -> Dict[str, StructType]:
        """Get OMOP CDM table schemas"""
        return get_cdm_table_schemas(cdm_version)

    def _get_synthea_table_schemas(self, synthea_version: str) -> Dict[str, StructType]:
        """Get Synthea table schemas"""
        return get_synthea_table_schemas(synthea_version)

    def _convert_synthea_data_types(self, df: DataFrame) -> DataFrame:
        """
        Convert Synthea data types equivalent to the R data type conversions
        """
        columns = df.columns
        
        # Convert date columns
        date_columns = ['START', 'STOP', 'DATE', 'START_DATE', 'END_DATE', 'BIRTHDATE', 'DEATHDATE']
        for col in date_columns:
            if col in columns:
                df = df.withColumn(col, to_date(col, 'yyyy-MM-dd'))
        
        # Convert code columns to string
        string_columns = ['CODE', 'REASONCODE', 'PHONE']
        for col in string_columns:
            if col in columns:
                df = df.withColumn(col, col(col).cast(StringType()))
        
        # Convert numeric columns
        numeric_columns = ['UTILIZATION']
        for col in numeric_columns:
            if col in columns:
                df = df.withColumn(col, col(col).cast(DoubleType()))
        
        return df

    def _convert_vocab_data_types(self, df: DataFrame, table_name: str) -> DataFrame:
        """Convert vocabulary table data types"""
        # Apply table-specific transformations
        if table_name == 'CONCEPT':
            df = df.withColumn('concept_id', col('concept_id').cast(IntegerType())) \
                   .withColumn('valid_start_date', to_date('valid_start_date', 'yyyy-MM-dd')) \
                   .withColumn('valid_end_date', to_date('valid_end_date', 'yyyy-MM-dd'))
        
        return df

    # All transformation methods delegate to SyntheaTransformations class
    def _transform_location(self):
        """Transform location table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_location()

    def _transform_care_site(self):
        """Transform care_site table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_care_site()

    def _transform_person(self):
        """Transform person table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_person()

    def _transform_observation_period(self):
        """Transform observation_period table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_observation_period()

    def _transform_provider(self):
        """Transform provider table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_provider()

    def _transform_visit_occurrence(self):
        """Transform visit_occurrence table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_visit_occurrence()

    def _transform_visit_detail(self):
        """Transform visit_detail table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_visit_detail()

    def _transform_condition_occurrence(self, synthea_version: str):
        """Transform condition_occurrence table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_condition_occurrence(synthea_version)

    def _transform_observation(self):
        """Transform observation table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_observation()

    def _transform_measurement(self, synthea_version: str):
        """Transform measurement table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_measurement(synthea_version)

    def _transform_procedure_occurrence(self, synthea_version: str):
        """Transform procedure_occurrence table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_procedure_occurrence(synthea_version)

    def _transform_drug_exposure(self):
        """Transform drug_exposure table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_drug_exposure()

    def _transform_condition_era(self):
        """Transform condition_era table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_condition_era()

    def _transform_drug_era(self):
        """Transform drug_era table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_drug_era()

    def _transform_cdm_source(self, cdm_version: str, cdm_source_name: str, 
                             cdm_source_abbreviation: str, cdm_holder: str, 
                             cdm_source_description: str, synthea_version: str):
        """Transform cdm_source table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_cdm_source(cdm_version, cdm_source_name, 
                                           cdm_source_abbreviation, cdm_holder, 
                                           cdm_source_description, synthea_version)

    def _transform_device_exposure(self):
        """Transform device_exposure table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_device_exposure()

    def _transform_death(self):
        """Transform death table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_death()

    def _transform_payer_plan_period(self, synthea_version: str):
        """Transform payer_plan_period table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_payer_plan_period(synthea_version)

    def _transform_cost(self, synthea_version: str):
        """Transform cost table"""
        transformations = SyntheaTransformations(self.spark, self.database_name)
        transformations.transform_cost(synthea_version)

    def close(self):
        """Close Spark session"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session closed")


def main():
    """
    Main function equivalent to the R codeToRun.R script
    """
    # Configuration - equivalent to the R script parameters
    config = {
        'cdm_version': "5.4",
        'database_name': "cdm_synthea_v540", 
        'synthea_schema': "synthea_v270",
        'synthea_file_loc': "/path/to/synthea/output/csv",  # Update with actual path
        'vocab_file_loc': "/path/to/vocab/csv",             # Update with actual path
        'synthea_version': "2.7.0"
    }
    
    # Initialize ETL processor
    etl = SyntheaOMOPETL(
        app_name="SyntheaOMOPETL", 
        database_name=config['database_name']
    )
    
    try:
        # Run the full ETL process
        etl.run_full_etl(
            cdm_version=config['cdm_version'],
            synthea_schema=config['synthea_schema'],
            synthea_file_loc=config['synthea_file_loc'],
            vocab_file_loc=config['vocab_file_loc'],
            synthea_version=config['synthea_version']
        )
        
        logger.info("ETL process completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        sys.exit(1)
        
    finally:
        # Clean up resources
        etl.close()


if __name__ == "__main__":
    main()
