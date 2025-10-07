#!/usr/bin/env python3
"""
Synthea Table Creator - PySpark Implementation
==============================================

This module contains the PySpark conversion of the R CreateSyntheaTables.r function.
It creates all Synthea staging tables using spark.sql() calls equivalent to the R SQL execution.
"""

from pyspark.sql import SparkSession
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class SyntheaTableCreator:
    """
    PySpark implementation of R CreateSyntheaTables function
    
    This class converts the R function that loads and executes SQL scripts to create
    Synthea staging tables. Instead of loading external SQL files, it generates
    CREATE TABLE statements directly and executes them via spark.sql().
    """
    
    def __init__(self, spark: SparkSession, database_name: str):
        """
        Initialize the table creator
        
        Args:
            spark: SparkSession instance
            database_name: Target database name for tables
        """
        self.spark = spark
        self.database_name = database_name

    def create_synthea_tables(self, synthea_version: str = "2.7.0") -> None:
        """
        Create Synthea staging tables equivalent to R CreateSyntheaTables function
        
        This function replaces the R logic that:
        1. Determines SQL file path based on synthea version
        2. Loads create_synthea_tables.sql using SqlRender::loadRenderTranslateSql()
        3. Executes SQL via DatabaseConnector::executeSql()
        
        Instead, it:
        1. Generates CREATE TABLE SQL statements directly
        2. Executes them via spark.sql() calls
        3. Handles version differences in table schemas
        
        Args:
            synthea_version: Synthea version ("2.7.0", "3.0.0", "3.1.0", "3.2.0", "3.3.0")
            
        Raises:
            ValueError: If unsupported Synthea version is specified
            Exception: If table creation fails
        """
        logger.info(f"Creating Synthea v{synthea_version} tables...")
        
        # Validate supported version (equivalent to R version check)
        supported_versions = ["2.7.0", "3.0.0", "3.1.0", "3.2.0", "3.3.0"]
        if synthea_version not in supported_versions:
            raise ValueError(
                f'Invalid synthea version specified. Currently "{", ".join(supported_versions)}" are supported.'
            )
        
        # Get table definitions (equivalent to loading SQL file)
        tables_to_create = self._get_synthea_table_definitions(synthea_version)
        
        # Create each table (equivalent to R's executeSql)
        for table_name, create_sql in tables_to_create.items():
            try:
                sql_filename = f"synthea_version/v{synthea_version.replace('.', '')}/create_synthea_tables.sql"
                logger.info(f"Running {sql_filename} - Creating table: synthea_{table_name}")
                
                # Drop table if exists (equivalent to overwrite mode)
                self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.synthea_{table_name}")
                
                # Create the table using spark.sql() (equivalent to R's executeSql)
                self.spark.sql(create_sql)
                
                logger.info(f"Successfully created: synthea_{table_name}")
                
            except Exception as e:
                logger.error(f"Failed to create Synthea table {table_name}: {str(e)}")
                raise
        
        logger.info(f"All Synthea v{synthea_version} tables created successfully")

    def _get_synthea_table_definitions(self, synthea_version: str) -> Dict[str, str]:
        """
        Get Synthea table CREATE TABLE SQL statements for the specified version
        
        This replaces the R logic of loading create_synthea_tables.sql files.
        Each version has slightly different schemas to match Synthea CSV output changes.
        
        Args:
            synthea_version: Synthea version
            
        Returns:
            Dictionary mapping table names to CREATE TABLE SQL statements
        """
        
        # Base tables common to all versions
        base_tables = {
            'allergies': self._create_allergies_table(synthea_version),
            'careplans': self._create_careplans_table(),
            'conditions': self._create_conditions_table(synthea_version),
            'encounters': self._create_encounters_table(),
            'immunizations': self._create_immunizations_table(),
            'imaging_studies': self._create_imaging_studies_table(),
            'medications': self._create_medications_table(),
            'observations': self._create_observations_table(synthea_version),
            'organizations': self._create_organizations_table(),
            'patients': self._create_patients_table(synthea_version),
            'procedures': self._create_procedures_table(synthea_version),
            'providers': self._create_providers_table(synthea_version),
            'devices': self._create_devices_table(),
            'payer_transitions': self._create_payer_transitions_table(synthea_version),
            'payers': self._create_payers_table(synthea_version),
            'supplies': self._create_supplies_table()
        }
        
        # Add version-specific tables
        if synthea_version in ["3.3.0"]:
            # Add claims tables for newer versions
            base_tables['claims'] = self._create_claims_table()
            base_tables['claims_transactions'] = self._create_claims_transactions_table()
        
        return base_tables

    def _create_allergies_table(self, synthea_version: str) -> str:
        """Create allergies table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has additional allergy detail columns
            return f"""
                CREATE TABLE {self.database_name}.synthea_allergies (
                    start DATE,
                    stop DATE,
                    patient STRING,
                    encounter STRING,
                    code STRING,
                    system STRING,
                    description STRING,
                    type STRING,
                    category STRING,
                    reaction1 STRING,
                    description1 STRING,
                    severity1 STRING,
                    reaction2 STRING,
                    description2 STRING,
                    severity2 STRING
                ) USING DELTA
            """
        else:
            # v2.7.0 basic allergy structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_allergies (
                    start DATE,
                    stop DATE,
                    patient STRING,
                    encounter STRING,
                    code STRING,
                    description STRING
                ) USING DELTA
            """

    def _create_careplans_table(self) -> str:
        """Create careplans table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_careplans (
                id STRING,
                start DATE,
                stop DATE,
                patient STRING,
                encounter STRING,
                code STRING,
                description STRING,
                reasoncode STRING,
                reasondescription STRING
            ) USING DELTA
        """

    def _create_conditions_table(self, synthea_version: str) -> str:
        """Create conditions table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has system column
            return f"""
                CREATE TABLE {self.database_name}.synthea_conditions (
                    start DATE,
                    stop DATE,
                    patient STRING,
                    encounter STRING,
                    system STRING,
                    code STRING,
                    description STRING
                ) USING DELTA
            """
        else:
            # v2.7.0 basic condition structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_conditions (
                    start DATE,
                    stop DATE,
                    patient STRING,
                    encounter STRING,
                    code STRING,
                    description STRING
                ) USING DELTA
            """

    def _create_encounters_table(self) -> str:
        """Create encounters table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_encounters (
                id STRING,
                start DATE,
                stop DATE,
                patient STRING,
                organization STRING,
                provider STRING,
                payer STRING,
                encounterclass STRING,
                code STRING,
                description STRING,
                base_encounter_cost DECIMAL(10,2),
                total_claim_cost DECIMAL(10,2),
                payer_coverage DECIMAL(10,2),
                reasoncode STRING,
                reasondescription STRING
            ) USING DELTA
        """

    def _create_immunizations_table(self) -> str:
        """Create immunizations table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_immunizations (
                date DATE,
                patient STRING,
                encounter STRING,
                code STRING,
                description STRING,
                base_cost DECIMAL(10,2)
            ) USING DELTA
        """

    def _create_imaging_studies_table(self) -> str:
        """Create imaging_studies table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_imaging_studies (
                id STRING,
                date DATE,
                patient STRING,
                encounter STRING,
                series_uid STRING,
                bodysite_code STRING,
                bodysite_description STRING,
                modality_code STRING,
                modality_description STRING,
                instance_uid STRING,
                sop_code STRING,
                sop_description STRING,
                procedure_code STRING
            ) USING DELTA
        """

    def _create_medications_table(self) -> str:
        """Create medications table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_medications (
                start DATE,
                stop DATE,
                patient STRING,
                payer STRING,
                encounter STRING,
                code STRING,
                description STRING,
                base_cost DECIMAL(10,2),
                payer_coverage DECIMAL(10,2),
                dispenses INT,
                totalcost DECIMAL(10,2),
                reasoncode STRING,
                reasondescription STRING
            ) USING DELTA
        """

    def _create_observations_table(self, synthea_version: str) -> str:
        """Create observations table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has category column
            return f"""
                CREATE TABLE {self.database_name}.synthea_observations (
                    date DATE,
                    patient STRING,
                    encounter STRING,
                    category STRING,
                    code STRING,
                    description STRING,
                    value STRING,
                    units STRING,
                    type STRING
                ) USING DELTA
            """
        else:
            # v2.7.0 basic observation structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_observations (
                    date DATE,
                    patient STRING,
                    encounter STRING,
                    code STRING,
                    description STRING,
                    value STRING,
                    units STRING,
                    type STRING
                ) USING DELTA
            """

    def _create_organizations_table(self) -> str:
        """Create organizations table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_organizations (
                id STRING,
                name STRING,
                address STRING,
                city STRING,
                state STRING,
                zip STRING,
                lat DECIMAL(10,6),
                lon DECIMAL(10,6),
                phone STRING,
                revenue DECIMAL(12,2),
                utilization STRING
            ) USING DELTA
        """

    def _create_patients_table(self, synthea_version: str) -> str:
        """Create patients table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has additional columns: middle, fips, income
            return f"""
                CREATE TABLE {self.database_name}.synthea_patients (
                    id STRING,
                    birthdate DATE,
                    deathdate DATE,
                    ssn STRING,
                    drivers STRING,
                    passport STRING,
                    prefix STRING,
                    first STRING,
                    middle STRING,
                    last STRING,
                    suffix STRING,
                    maiden STRING,
                    marital STRING,
                    race STRING,
                    ethnicity STRING,
                    gender STRING,
                    birthplace STRING,
                    address STRING,
                    city STRING,
                    state STRING,
                    county STRING,
                    fips STRING,
                    zip STRING,
                    lat DECIMAL(10,6),
                    lon DECIMAL(10,6),
                    healthcare_expenses DECIMAL(12,2),
                    healthcare_coverage DECIMAL(12,2),
                    income INT
                ) USING DELTA
            """
        else:
            # v2.7.0 basic patient structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_patients (
                    id STRING,
                    birthdate DATE,
                    deathdate DATE,
                    ssn STRING,
                    drivers STRING,
                    passport STRING,
                    prefix STRING,
                    first STRING,
                    last STRING,
                    suffix STRING,
                    maiden STRING,
                    marital STRING,
                    race STRING,
                    ethnicity STRING,
                    gender STRING,
                    birthplace STRING,
                    address STRING,
                    city STRING,
                    state STRING,
                    county STRING,
                    zip STRING,
                    lat DECIMAL(10,6),
                    lon DECIMAL(10,6),
                    healthcare_expenses DECIMAL(12,2),
                    healthcare_coverage DECIMAL(12,2)
                ) USING DELTA
            """

    def _create_procedures_table(self, synthea_version: str) -> str:
        """Create procedures table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has start/stop dates and system column
            return f"""
                CREATE TABLE {self.database_name}.synthea_procedures (
                    start DATE,
                    stop DATE,
                    patient STRING,
                    encounter STRING,
                    system STRING,
                    code STRING,
                    description STRING,
                    base_cost DECIMAL(10,2),
                    reasoncode STRING,
                    reasondescription STRING
                ) USING DELTA
            """
        else:
            # v2.7.0 has single date column
            return f"""
                CREATE TABLE {self.database_name}.synthea_procedures (
                    date DATE,
                    patient STRING,
                    encounter STRING,
                    code STRING,
                    description STRING,
                    base_cost DECIMAL(10,2),
                    reasoncode STRING,
                    reasondescription STRING
                ) USING DELTA
            """

    def _create_providers_table(self, synthea_version: str) -> str:
        """Create providers table with version-specific columns"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has encounters and procedures counts
            return f"""
                CREATE TABLE {self.database_name}.synthea_providers (
                    id STRING,
                    organization STRING,
                    name STRING,
                    gender STRING,
                    speciality STRING,
                    address STRING,
                    city STRING,
                    state STRING,
                    zip STRING,
                    lat DECIMAL(10,6),
                    lon DECIMAL(10,6),
                    encounters INT,
                    procedures INT
                ) USING DELTA
            """
        else:
            # v2.7.0 has utilization column
            return f"""
                CREATE TABLE {self.database_name}.synthea_providers (
                    id STRING,
                    organization STRING,
                    name STRING,
                    gender STRING,
                    speciality STRING,
                    address STRING,
                    city STRING,
                    state STRING,
                    zip STRING,
                    lat DECIMAL(10,6),
                    lon DECIMAL(10,6),
                    utilization DECIMAL(10,2)
                ) USING DELTA
            """

    def _create_devices_table(self) -> str:
        """Create devices table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_devices (
                start DATE,
                stop DATE,
                patient STRING,
                encounter STRING,
                code STRING,
                description STRING,
                udi STRING
            ) USING DELTA
        """

    def _create_payer_transitions_table(self, synthea_version: str) -> str:
        """Create payer_transitions table with version-specific schema"""
        if synthea_version in ["3.3.0"]:
            # v3.3.0 has detailed payer transition structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_payer_transitions (
                    patient STRING,
                    memberid STRING,
                    start_date DATE,
                    end_date DATE,
                    payer STRING,
                    secondary_payer STRING,
                    plan_ownership STRING,
                    owner_name STRING
                ) USING DELTA
            """
        else:
            # v2.7.0 has basic year-based structure
            return f"""
                CREATE TABLE {self.database_name}.synthea_payer_transitions (
                    patient STRING,
                    start_year DECIMAL(4,0),
                    end_year DECIMAL(4,0),
                    payer STRING,
                    ownership STRING
                ) USING DELTA
            """

    def _create_payers_table(self, synthea_version: str) -> str:
        """Create payers table with version-specific columns"""
        ownership_column = "ownership STRING," if synthea_version in ["3.3.0"] else ""
        
        return f"""
            CREATE TABLE {self.database_name}.synthea_payers (
                id STRING,
                name STRING,
                {ownership_column}
                address STRING,
                city STRING,
                state_headquartered STRING,
                zip STRING,
                phone STRING,
                amount_covered DECIMAL(12,2),
                amount_uncovered DECIMAL(12,2),
                revenue DECIMAL(12,2),
                covered_encounters DECIMAL(10,0),
                uncovered_encounters DECIMAL(10,0),
                covered_medications DECIMAL(10,0),
                uncovered_medications DECIMAL(10,0),
                covered_procedures DECIMAL(10,0),
                uncovered_procedures DECIMAL(10,0),
                covered_immunizations DECIMAL(10,0),
                uncovered_immunizations DECIMAL(10,0),
                unique_customers DECIMAL(10,0),
                qols_avg DECIMAL(8,4),
                member_months DECIMAL(10,0)
            ) USING DELTA
        """

    def _create_supplies_table(self) -> str:
        """Create supplies table (consistent across versions)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_supplies (
                date DATE,
                patient STRING,
                encounter STRING,
                code STRING,
                description STRING,
                quantity DECIMAL(10,2)
            ) USING DELTA
        """

    def _create_claims_table(self) -> str:
        """Create claims table (v3.3.0 only)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_claims (
                id STRING,
                patientid STRING,
                providerid STRING,
                primarypatientinsuranceid STRING,
                secondarypatientinsuranceid STRING,
                departmentid STRING,
                patientdepartmentid STRING,
                diagnosis1 STRING,
                diagnosis2 STRING,
                diagnosis3 STRING,
                diagnosis4 STRING,
                diagnosis5 STRING,
                diagnosis6 STRING,
                diagnosis7 STRING,
                diagnosis8 STRING,
                referringproviderid STRING,
                appointmentid STRING,
                currentillnessdate DATE,
                servicedate DATE,
                supervisingproviderid STRING,
                status1 STRING,
                status2 STRING,
                statusp STRING,
                outstanding1 DECIMAL(10,2),
                outstanding2 DECIMAL(10,2),
                outstandingp DECIMAL(10,2),
                lastbilleddate1 DATE,
                lastbilleddate2 DATE,
                lastbilleddatep DATE,
                healthcareclaimtypeid1 INT,
                healthcareclaimtypeid2 INT
            ) USING DELTA
        """

    def _create_claims_transactions_table(self) -> str:
        """Create claims_transactions table (v3.3.0 only)"""
        return f"""
            CREATE TABLE {self.database_name}.synthea_claims_transactions (
                id STRING,
                claimid STRING,
                chargeid INT,
                patientid STRING,
                type STRING,
                amount DECIMAL(10,2),
                method STRING,
                fromdate DATE,
                todate DATE,
                placeofservice STRING,
                procedurecode STRING,
                modifier1 STRING,
                modifier2 STRING,
                diagnosisref1 INT,
                diagnosisref2 INT,
                diagnosisref3 INT,
                diagnosisref4 INT,
                units DECIMAL(10,2),
                departmentid INT,
                notes STRING,
                unitamount DECIMAL(10,2),
                transferoutid INT,
                transfertype STRING,
                payments DECIMAL(10,2),
                adjustments DECIMAL(10,2),
                transfers DECIMAL(10,2),
                outstanding DECIMAL(10,2),
                appointmentid STRING,
                linenote STRING,
                patientinsuranceid STRING,
                feescheduleid INT,
                providerid STRING,
                supervisingproviderid STRING
            ) USING DELTA
        """


# Convenience function for direct usage
def create_synthea_tables(spark: SparkSession, 
                         database_name: str, 
                         synthea_version: str = "2.7.0") -> None:
    """
    Convenience function equivalent to R CreateSyntheaTables function
    
    Args:
        spark: SparkSession (equivalent to R connectionDetails)
        database_name: Database name (equivalent to R syntheaSchema)
        synthea_version: Synthea version (equivalent to R syntheaVersion parameter)
    """
    creator = SyntheaTableCreator(spark, database_name)
    creator.create_synthea_tables(synthea_version)
