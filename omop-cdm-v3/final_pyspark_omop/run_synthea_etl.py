#!/usr/bin/env python3
"""
Synthea OMOP ETL Runner Script
=============================

This is the main entry point script equivalent to the R codeToRun.R file.
Run this script to perform the complete Synthea to OMOP CDM conversion.

Usage:
    python run_synthea_etl.py --config config.yaml
    python run_synthea_etl.py --help

Author: Converted from R ETLSyntheaBuilder package
"""

import argparse
import yaml
import sys
import os
import logging
from pathlib import Path

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from synthea_omop_etl import SyntheaOMOPETL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('synthea_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path: str) -> dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        sys.exit(1)

def validate_paths(config: dict) -> bool:
    """Validate that required file paths exist"""
    required_paths = ['synthea_file_loc', 'vocab_file_loc']
    
    for path_key in required_paths:
        if path_key in config and config[path_key]:
            path = Path(config[path_key])
            if not path.exists():
                logger.error(f"Path does not exist: {config[path_key]}")
                return False
            if not path.is_dir():
                logger.error(f"Path is not a directory: {config[path_key]}")
                return False
    
    return True

def create_default_config() -> str:
    """Create a default configuration file template"""
    default_config = {
        'spark_config': {
            'app_name': 'SyntheaOMOPETL',
            'warehouse_dir': '/tmp/spark-warehouse',
            'database_name': 'cdm_synthea_v540'
        },
        'etl_config': {
            'cdm_version': '5.4',
            'synthea_version': '2.7.0',
            'synthea_schema': 'synthea_v270',
            'synthea_file_loc': '/path/to/synthea/output/csv',
            'vocab_file_loc': '/path/to/vocab/csv'
        },
        'cdm_source_config': {
            'cdm_source_name': 'Synthea synthetic health database',
            'cdm_source_abbreviation': 'Synthea',
            'cdm_holder': 'OHDSI',
            'cdm_source_description': 'SyntheaTM is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats.'
        },
        'performance_config': {
            'create_extra_indices': True,
            'cache_tables': True
        }
    }
    
    config_file = 'synthea_etl_config.yaml'
    with open(config_file, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False, sort_keys=False)
    
    return config_file

def run_etl_with_config(config: dict) -> None:
    """Run the ETL process with the given configuration"""
    spark_config = config.get('spark_config', {})
    etl_config = config.get('etl_config', {})
    source_config = config.get('cdm_source_config', {})
    
    # Initialize ETL processor
    etl = SyntheaOMOPETL(
        app_name=spark_config.get('app_name', 'SyntheaOMOPETL'),
        warehouse_dir=spark_config.get('warehouse_dir'),
        database_name=spark_config.get('database_name', 'omop_cdm')
    )
    
    try:
        logger.info("="*60)
        logger.info("Starting Synthea to OMOP CDM ETL Process")
        logger.info("="*60)
        
        # Validate configuration
        if not validate_paths(etl_config):
            logger.error("Path validation failed. Please check your configuration.")
            sys.exit(1)
        
        # Run the complete ETL workflow
        etl.run_full_etl(
            cdm_version=etl_config.get('cdm_version', '5.4'),
            synthea_schema=etl_config.get('synthea_schema', 'synthea_v270'),
            synthea_file_loc=etl_config.get('synthea_file_loc'),
            vocab_file_loc=etl_config.get('vocab_file_loc'),
            synthea_version=etl_config.get('synthea_version', '2.7.0')
        )
        
        logger.info("="*60)
        logger.info("ETL Process completed successfully!")
        logger.info("="*60)
        
        # Optional: Run validation queries
        if config.get('run_validation', False):
            run_validation(etl)
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise
    finally:
        etl.close()

def run_validation(etl: SyntheaOMOPETL) -> None:
    """Run validation queries to check data quality"""
    logger.info("Running data validation checks...")
    
    validation_queries = [
        {
            'name': 'Person count',
            'query': f'SELECT COUNT(*) as person_count FROM {etl.database_name}.person'
        },
        {
            'name': 'Visit occurrence count',
            'query': f'SELECT COUNT(*) as visit_count FROM {etl.database_name}.visit_occurrence'
        },
        {
            'name': 'Condition occurrence count',
            'query': f'SELECT COUNT(*) as condition_count FROM {etl.database_name}.condition_occurrence'
        },
        {
            'name': 'Drug exposure count',
            'query': f'SELECT COUNT(*) as drug_count FROM {etl.database_name}.drug_exposure'
        },
        {
            'name': 'Gender distribution',
            'query': f'''
                SELECT 
                    gender_concept_id,
                    COUNT(*) as count
                FROM {etl.database_name}.person 
                GROUP BY gender_concept_id
            '''
        },
        {
            'name': 'Visit type distribution',
            'query': f'''
                SELECT 
                    visit_concept_id,
                    COUNT(*) as count
                FROM {etl.database_name}.visit_occurrence 
                GROUP BY visit_concept_id
            '''
        }
    ]
    
    for validation in validation_queries:
        try:
            logger.info(f"Running validation: {validation['name']}")
            result = etl.spark.sql(validation['query']).collect()
            
            if len(result) == 1 and 'count' in result[0].asDict():
                logger.info(f"  Result: {result[0]['count']} records")
            else:
                logger.info(f"  Results:")
                for row in result[:5]:  # Show first 5 rows
                    logger.info(f"    {row.asDict()}")
                if len(result) > 5:
                    logger.info(f"    ... and {len(result) - 5} more rows")
                    
        except Exception as e:
            logger.warning(f"Validation query failed for '{validation['name']}': {str(e)}")
    
    logger.info("Validation checks completed")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Run Synthea to OMOP CDM ETL conversion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --config my_config.yaml
  %(prog)s --create-config
  %(prog)s --help
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to YAML configuration file'
    )
    
    parser.add_argument(
        '--create-config',
        action='store_true',
        help='Create a default configuration file template and exit'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Run validation queries only (requires existing data)'
    )
    
    args = parser.parse_args()
    
    # Create default config if requested
    if args.create_config:
        config_file = create_default_config()
        logger.info(f"Created default configuration file: {config_file}")
        logger.info("Please edit this file with your specific paths and settings before running the ETL.")
        return
    
    # Require config file for ETL execution
    if not args.config:
        logger.error("Configuration file is required. Use --config to specify a file, or --create-config to generate a template.")
        parser.print_help()
        sys.exit(1)
    
    # Load and run with configuration
    config = load_config(args.config)
    
    if args.validate_only:
        # Run validation queries only
        spark_config = config.get('spark_config', {})
        etl = SyntheaOMOPETL(
            app_name=spark_config.get('app_name', 'SyntheaOMOPETL'),
            warehouse_dir=spark_config.get('warehouse_dir'),
            database_name=spark_config.get('database_name', 'omop_cdm')
        )
        try:
            run_validation(etl)
        finally:
            etl.close()
    else:
        # Run full ETL process
        run_etl_with_config(config)

if __name__ == "__main__":
    main()
