#!/usr/bin/env python3
"""
Vocabulary Loader - PySpark Implementation
==========================================

This module contains the PySpark conversion of the R LoadVocabFromCsv.r function.
It loads OMOP vocabulary CSV files using PySpark with chunking and type conversion.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class VocabularyLoader:
    """
    PySpark implementation of R LoadVocabFromCsv function
    
    This class converts the R function that loads vocabulary CSV files with
    chunking, type conversion, and special handling for different vocab tables.
    """
    
    def __init__(self, spark: SparkSession, database_name: str):
        """
        Initialize the vocabulary loader
        
        Args:
            spark: SparkSession instance
            database_name: Target database name for vocabulary tables
        """
        self.spark = spark
        self.database_name = database_name

    def load_vocab_from_csv(self, 
                           vocab_file_loc: str,
                           bulk_load: bool = False,
                           delimiter: str = "\\t",
                           chunk_size: int = 10000000) -> None:
        """
        Load vocabulary CSV files equivalent to R LoadVocabFromCsv function
        
        This function replaces the R logic that:
        1. Defines a list of expected vocabulary CSV files  
        2. Uses data.table::fread() with tab delimiter to read files
        3. Applies special date handling for concept tables
        4. Uses readr::type_convert() for type inference
        5. Handles drug_strength table null value replacement
        6. Chunks large files for insertion via DatabaseConnector::insertTable()
        
        Instead, it:
        1. Uses the same vocabulary file list
        2. Uses spark.read.csv() with appropriate options
        3. Applies PySpark DataFrame date transformations
        4. Uses PySpark type casting and null handling
        5. Leverages PySpark's distributed processing (no manual chunking needed)
        6. Uses DataFrame.write.saveAsTable() for persistence
        
        Args:
            vocab_file_loc: Path to vocabulary CSV files directory
            bulk_load: Whether to use bulk loading (PySpark auto-optimizes)
            delimiter: CSV delimiter (default tab "\\t" like R)
            chunk_size: Chunk size for processing (PySpark handles automatically)
            
        Raises:
            FileNotFoundError: If vocab_file_loc doesn't exist
            Exception: If vocabulary loading fails
        """
        logger.info(f"Loading vocabulary tables from: {vocab_file_loc}")
        
        # Define expected vocabulary CSV files (same as R csvList)
        expected_csv_files = [
            "concept.csv",
            "vocabulary.csv", 
            "concept_ancestor.csv",
            "concept_relationship.csv",
            "relationship.csv",
            "concept_synonym.csv",
            "domain.csv",
            "concept_class.csv", 
            "drug_strength.csv"
        ]
        
        # Validate vocabulary file location (equivalent to R file.exists check)
        if not os.path.exists(vocab_file_loc):
            raise FileNotFoundError(
                f"Vocabulary File Location specified is invalid: {vocab_file_loc}. "
                "Please provide a valid fully qualified (absolute) path to the directory."
            )
        
        # Get available files (equivalent to R fileList filtering)
        available_files = os.listdir(vocab_file_loc)
        files_to_process = [f for f in available_files if f.lower() in [csv.lower() for csv in expected_csv_files]]
        
        if not files_to_process:
            logger.warning(f"No vocabulary CSV files found in {vocab_file_loc}")
            return
        
        logger.info(f"Found {len(files_to_process)} vocabulary files to process")
        
        # Process each vocabulary file (equivalent to R for loop)
        for csv_file in files_to_process:
            file_path = os.path.join(vocab_file_loc, csv_file)
            table_name = csv_file.replace('.csv', '').lower()
            
            try:
                logger.info(f"Working on file {file_path}")
                
                # Read CSV file (equivalent to R data.table::fread)
                logger.info(" - reading file")
                df = self._read_vocabulary_csv(file_path, delimiter)
                
                # Apply vocabulary-specific transformations
                df = self._convert_vocabulary_data_types(df, csv_file.lower(), table_name)
                
                # Clear existing data (equivalent to R DELETE FROM statement)
                self._clear_vocabulary_table(table_name)
                
                # Save vocabulary data (equivalent to R chunked insertTable)
                self._save_vocabulary_table(df, table_name, bulk_load, chunk_size)
                
                logger.info(" - Success")
                
            except Exception as e:
                logger.error(f"Failed to load vocabulary file {csv_file}: {str(e)}")
                raise
        
        logger.info("All vocabulary tables loaded successfully")

    def _read_vocabulary_csv(self, file_path: str, delimiter: str) -> DataFrame:
        """
        Read vocabulary CSV file with proper options
        
        Equivalent to R data.table::fread() with vocabulary-specific options:
        - header = TRUE -> header="true"
        - sep = delimiter -> sep=delimiter (usually tab)
        - na.strings = "" -> nullValue=""
        """
        return self.spark.read \\
            .option("header", "true") \\
            .option("inferSchema", "false") \\
            .option("sep", delimiter) \\
            .option("nullValue", "") \\
            .option("emptyValue", "") \\
            .option("quote", '"') \\
            .option("escape", '"') \\
            .option("multiline", "true") \\
            .csv(file_path)

    def _convert_vocabulary_data_types(self, df: DataFrame, csv_file: str, table_name: str) -> DataFrame:
        """
        Convert vocabulary data types with table-specific logic
        
        This replaces the complex R logic for:
        1. Special date handling for concept, concept_relationship, drug_strength tables
        2. readr::type_convert() for automatic type inference
        3. drug_strength table null value replacement with mutate_at
        """
        columns = df.columns
        
        # Special date handling for tables with valid_start_date/valid_end_date
        # (equivalent to R date conversion logic)
        if csv_file in ["concept.csv", "concept_relationship.csv", "drug_strength.csv"]:
            logger.info(" - handling dates")
            
            # Convert YYYYMMDD format to DATE (equivalent to R as.Date with %Y%m%d format)
            if 'valid_start_date' in columns:
                df = df.withColumn('valid_start_date', 
                    when(col('valid_start_date').isNull() | (col('valid_start_date') == ""), None)
                    .otherwise(to_date(col('valid_start_date'), 'yyyyMMdd')))
            
            if 'valid_end_date' in columns:
                df = df.withColumn('valid_end_date',
                    when(col('valid_end_date').isNull() | (col('valid_end_date') == ""), None) 
                    .otherwise(to_date(col('valid_end_date'), 'yyyyMMdd')))
        
        # Apply type conversion (equivalent to R readr::type_convert)
        logger.info(" - type converting")
        df = self._apply_vocabulary_type_conversion(df, table_name)
        
        # Special handling for drug_strength table (equivalent to R mutate_at with replace)
        if csv_file == "drug_strength.csv":
            logger.info(" - handling drug_strength null values")
            drug_strength_numeric_cols = [
                "amount_value", "amount_unit_concept_id", "numerator_value",
                "numerator_unit_concept_id", "denominator_value", 
                "denominator_unit_concept_id", "box_size"
            ]
            
            for col_name in drug_strength_numeric_cols:
                if col_name in columns:
                    # Replace NA with 0 (equivalent to R mutate_at with replace)
                    df = df.withColumn(col_name,
                        when(col(col_name).isNull() | (col(col_name) == ""), 0)
                        .otherwise(col(col_name).cast(DoubleType())))
        
        return df

    def _apply_vocabulary_type_conversion(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply appropriate data types for vocabulary tables
        
        This replaces R's readr::type_convert() with explicit type casting
        based on OMOP vocabulary table specifications.
        """
        columns = df.columns
        
        # Common vocabulary table type mappings
        type_mappings = {
            'concept': {
                'concept_id': IntegerType(),
                'concept_name': StringType(),
                'domain_id': StringType(), 
                'vocabulary_id': StringType(),
                'concept_class_id': StringType(),
                'standard_concept': StringType(),
                'concept_code': StringType(),
                'invalid_reason': StringType()
            },
            'vocabulary': {
                'vocabulary_id': StringType(),
                'vocabulary_name': StringType(),
                'vocabulary_reference': StringType(),
                'vocabulary_version': StringType(),
                'vocabulary_concept_id': IntegerType()
            },
            'concept_relationship': {
                'concept_id_1': IntegerType(),
                'concept_id_2': IntegerType(), 
                'relationship_id': StringType(),
                'invalid_reason': StringType()
            },
            'concept_ancestor': {
                'ancestor_concept_id': IntegerType(),
                'descendant_concept_id': IntegerType(),
                'min_levels_of_separation': IntegerType(),
                'max_levels_of_separation': IntegerType()
            },
            'drug_strength': {
                'drug_concept_id': IntegerType(),
                'ingredient_concept_id': IntegerType(),
                'amount_value': DoubleType(),
                'amount_unit_concept_id': IntegerType(),
                'numerator_value': DoubleType(),
                'numerator_unit_concept_id': IntegerType(),
                'denominator_value': DoubleType(),
                'denominator_unit_concept_id': IntegerType(),
                'box_size': IntegerType(),
                'invalid_reason': StringType()
            }
        }
        
        # Apply type conversions if mapping exists for this table
        if table_name in type_mappings:
            for col_name, data_type in type_mappings[table_name].items():
                if col_name in columns:
                    df = df.withColumn(col_name, col(col_name).cast(data_type))
        
        # Apply common integer casting for _concept_id and _id columns
        for col_name in columns:
            if col_name.endswith('_concept_id') or col_name.endswith('_id'):
                if col_name not in ['vocabulary_id', 'domain_id', 'relationship_id']:
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
        
        return df

    def _clear_vocabulary_table(self, table_name: str) -> None:
        """
        Clear existing data from vocabulary table
        
        Equivalent to R SQL: "DELETE FROM @table_name;"
        """
        target_table = f"{self.database_name}.{table_name}"
        
        try:
            # Check if table exists
            self.spark.sql(f"DESCRIBE TABLE {target_table}")
            
            # Clear table contents (equivalent to DELETE FROM)
            self.spark.sql(f"DELETE FROM {target_table}")
            logger.info(f" - cleared existing data from {target_table}")
            
        except Exception as e:
            # Table doesn't exist or other error - that's ok for first load
            logger.info(f" - table {target_table} doesn't exist or couldn't be cleared: {str(e)}")

    def _save_vocabulary_table(self, df: DataFrame, table_name: str, 
                              bulk_load: bool, chunk_size: int) -> None:
        """
        Save vocabulary DataFrame to table with chunking logic
        
        This replaces the R chunking logic that:
        1. Calculates numberOfChunks = ceiling(nrow / chunkSize)
        2. Processes data in chunks with for loop  
        3. Uses DatabaseConnector::insertTable() for each chunk
        
        PySpark handles large datasets automatically, but we can still
        implement chunking for very large vocabulary tables if needed.
        """
        target_table = f"{self.database_name}.{table_name}"
        row_count = df.count()
        
        logger.info(f" - uploading {row_count:,} rows of data to {table_name}")
        
        if row_count > chunk_size:
            # For very large vocabulary tables, process in chunks
            logger.info(f" - processing in chunks of {chunk_size:,} rows")
            self._save_vocabulary_table_chunked(df, target_table, chunk_size, row_count)
        else:
            # For smaller tables, process all at once
            df.write \\
              .mode("overwrite") \\
              .option("overwriteSchema", "true") \\
              .saveAsTable(target_table)
            
            logger.info(f" - successfully uploaded {row_count:,} rows to {target_table}")

    def _save_vocabulary_table_chunked(self, df: DataFrame, target_table: str, 
                                      chunk_size: int, total_rows: int) -> None:
        """
        Save vocabulary table in chunks (for very large vocab tables)
        
        Equivalent to R chunking logic with startRow and maxRows calculation.
        """
        import math
        
        num_chunks = math.ceil(total_rows / chunk_size)
        logger.info(f" - processing {total_rows:,} rows in {num_chunks} chunks")
        
        # Add row numbers for chunking
        from pyspark.sql.window import Window
        window_spec = Window.orderBy(lit(1))  # Arbitrary ordering for row_number
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        
        # Process each chunk
        for chunk_num in range(num_chunks):
            start_row = chunk_num * chunk_size + 1
            end_row = min((chunk_num + 1) * chunk_size, total_rows)
            
            logger.info(f" - chunk uploading started for rows {start_row:,} to {end_row:,}")
            
            chunk_df = df_with_row_num.filter(
                (col("row_num") >= start_row) & (col("row_num") <= end_row)
            ).drop("row_num")
            
            # First chunk overwrites, subsequent chunks append
            write_mode = "overwrite" if chunk_num == 0 else "append"
            
            chunk_df.write \\
              .mode(write_mode) \\
              .option("overwriteSchema", "true") \\
              .saveAsTable(target_table)
            
            logger.info(f" - chunk {chunk_num + 1}/{num_chunks} completed")

    def get_vocabulary_summary(self) -> Dict[str, int]:
        """
        Get summary of loaded vocabulary tables
        
        Returns:
            Dictionary mapping vocabulary table names to row counts
        """
        vocab_tables = [
            'concept', 'vocabulary', 'concept_ancestor', 'concept_relationship',
            'relationship', 'concept_synonym', 'domain', 'concept_class', 'drug_strength'
        ]
        
        summary = {}
        for table_name in vocab_tables:
            try:
                count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.{table_name}").collect()[0]['count']
                summary[table_name] = count
            except Exception as e:
                logger.warning(f"Could not get count for {table_name}: {str(e)}")
                summary[table_name] = -1
        
        return summary

    def validate_vocabulary_data(self) -> Dict[str, List[str]]:
        """
        Validate loaded vocabulary data quality
        
        Returns:
            Dictionary of validation results
        """
        validation_results = {
            'warnings': [],
            'errors': [],
            'info': []
        }
        
        try:
            # Check concept table
            concept_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.concept").collect()[0]['count']
            if concept_count == 0:
                validation_results['errors'].append("No concepts found in concept table")
            else:
                validation_results['info'].append(f"Found {concept_count:,} concepts")
            
            # Check for standard concepts
            std_concept_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.concept WHERE standard_concept = 'S'").collect()[0]['count']
            validation_results['info'].append(f"Found {std_concept_count:,} standard concepts")
            
            # Check vocabulary table
            vocab_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {self.database_name}.vocabulary").collect()[0]['count']
            if vocab_count == 0:
                validation_results['warnings'].append("No vocabularies found in vocabulary table")
            else:
                validation_results['info'].append(f"Found {vocab_count:,} vocabularies")
            
        except Exception as e:
            validation_results['errors'].append(f"Vocabulary validation failed: {str(e)}")
        
        return validation_results


# Convenience function for direct usage
def load_vocab_from_csv(spark: SparkSession, 
                       database_name: str,
                       vocab_file_loc: str,
                       bulk_load: bool = False,
                       delimiter: str = "\\t") -> None:
    """
    Convenience function equivalent to R LoadVocabFromCsv function
    
    Args:
        spark: SparkSession (equivalent to R connectionDetails)
        database_name: Database name (equivalent to R cdmSchema)
        vocab_file_loc: Path to vocabulary CSV files (equivalent to R vocabFileLoc)
        bulk_load: Bulk loading flag (equivalent to R bulkLoad parameter) 
        delimiter: CSV delimiter (equivalent to R delimiter parameter)
    """
    loader = VocabularyLoader(spark, database_name)
    loader.load_vocab_from_csv(vocab_file_loc, bulk_load, delimiter)
