#!/usr/bin/env python3
"""
Synthea OMOP Transformation Functions - PySpark Implementation
============================================================

This module contains all the SQL transformation functions converted from R SQL files.
Each function implements the equivalent logic from the inst/sql/sql_server files.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)

class SyntheaTransformations:
    """
    Contains all transformation functions for converting Synthea data to OMOP CDM
    """
    
    def __init__(self, spark: SparkSession, database_name: str):
        self.spark = spark
        self.database_name = database_name
    
    def create_source_to_standard_vocab_map(self) -> None:
        """
        Create source to standard vocabulary mapping table
        Equivalent to create_source_to_standard_vocab_map.sql
        """
        logger.info("Creating source_to_standard_vocab_map table...")
        
        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.source_to_standard_vocab_map")
        
        # Create the mapping table using SQL equivalent of the CTE logic
        create_map_sql = f"""
        WITH CTE_VOCAB_MAP AS (
            SELECT 
                c.concept_code AS SOURCE_CODE, 
                c.concept_id AS SOURCE_CONCEPT_ID, 
                c.concept_name AS SOURCE_CODE_DESCRIPTION, 
                c.vocabulary_id AS SOURCE_VOCABULARY_ID,
                c.domain_id AS SOURCE_DOMAIN_ID, 
                c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
                c.valid_start_date AS SOURCE_VALID_START_DATE, 
                c.valid_end_date AS SOURCE_VALID_END_DATE, 
                c.invalid_reason AS SOURCE_INVALID_REASON,
                c1.concept_id AS TARGET_CONCEPT_ID, 
                c1.concept_name AS TARGET_CONCEPT_NAME, 
                c1.vocabulary_id AS TARGET_VOCABULARY_ID, 
                c1.domain_id AS TARGET_DOMAIN_ID, 
                c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
                c1.invalid_reason AS TARGET_INVALID_REASON, 
                c1.standard_concept AS TARGET_STANDARD_CONCEPT
            FROM {self.database_name}.concept c
            JOIN {self.database_name}.concept_relationship cr
                ON c.concept_id = cr.concept_id_1
                AND cr.invalid_reason IS NULL
                AND lower(cr.relationship_id) = 'maps to'
            JOIN {self.database_name}.concept c1
                ON cr.concept_id_2 = c1.concept_id
                AND c1.invalid_reason IS NULL
                
            UNION ALL
            
            SELECT 
                stcm.source_code, 
                stcm.source_concept_id, 
                stcm.source_code_description, 
                stcm.source_vocabulary_id, 
                c1.domain_id AS SOURCE_DOMAIN_ID, 
                c2.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
                c1.valid_start_date AS SOURCE_VALID_START_DATE, 
                c1.valid_end_date AS SOURCE_VALID_END_DATE,
                stcm.invalid_reason AS SOURCE_INVALID_REASON,
                stcm.target_concept_id, 
                c2.concept_name AS TARGET_CONCEPT_NAME, 
                stcm.target_vocabulary_id, 
                c2.domain_id AS TARGET_DOMAIN_ID, 
                c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
                c2.invalid_reason AS TARGET_INVALID_REASON, 
                c2.standard_concept AS TARGET_STANDARD_CONCEPT
            FROM {self.database_name}.source_to_concept_map stcm
            LEFT OUTER JOIN {self.database_name}.concept c1
                ON c1.concept_id = stcm.source_concept_id
            LEFT OUTER JOIN {self.database_name}.concept c2
                ON c2.concept_id = stcm.target_concept_id
            WHERE stcm.invalid_reason IS NULL
        )
        CREATE TABLE {self.database_name}.source_to_standard_vocab_map AS
        SELECT * FROM CTE_VOCAB_MAP
        """
        
        self.spark.sql(create_map_sql)
        logger.info("Created source_to_standard_vocab_map table")

    def create_source_to_source_vocab_map(self) -> None:
        """
        Create source to source vocabulary mapping table
        Equivalent to create_source_to_source_vocab_map.sql
        """
        logger.info("Creating source_to_source_vocab_map table...")
        
        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.source_to_source_vocab_map")
        
        # Create the source to source mapping
        create_source_map_sql = f"""
        CREATE TABLE {self.database_name}.source_to_source_vocab_map AS
        SELECT DISTINCT
            c1.concept_code AS SOURCE_CODE,
            c1.concept_id AS SOURCE_CONCEPT_ID,
            c1.concept_name AS SOURCE_CODE_DESCRIPTION,
            c1.vocabulary_id AS SOURCE_VOCABULARY_ID,
            c1.domain_id AS SOURCE_DOMAIN_ID,
            c1.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
            c1.valid_start_date AS SOURCE_VALID_START_DATE,
            c1.valid_end_date AS SOURCE_VALID_END_DATE,
            c1.invalid_reason AS SOURCE_INVALID_REASON
        FROM {self.database_name}.concept c1
        WHERE c1.invalid_reason IS NULL
        
        UNION ALL
        
        SELECT DISTINCT
            stcm.source_code,
            stcm.source_concept_id,
            stcm.source_code_description,
            stcm.source_vocabulary_id,
            c1.domain_id AS SOURCE_DOMAIN_ID,
            c1.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
            c1.valid_start_date AS SOURCE_VALID_START_DATE,
            c1.valid_end_date AS SOURCE_VALID_END_DATE,
            stcm.invalid_reason AS SOURCE_INVALID_REASON
        FROM {self.database_name}.source_to_concept_map stcm
        LEFT OUTER JOIN {self.database_name}.concept c1
            ON c1.concept_id = stcm.source_concept_id
        WHERE stcm.invalid_reason IS NULL
        """
        
        self.spark.sql(create_source_map_sql)
        logger.info("Created source_to_source_vocab_map table")

    def create_states_map(self) -> None:
        """
        Create states mapping table for location standardization
        Equivalent to create_states_map.sql
        """
        logger.info("Creating states_map table...")
        
        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.states_map")
        
        # Create states mapping with common US state abbreviations
        states_data = [
            ('Alabama', 'AL'), ('Alaska', 'AK'), ('Arizona', 'AZ'), ('Arkansas', 'AR'),
            ('California', 'CA'), ('Colorado', 'CO'), ('Connecticut', 'CT'), ('Delaware', 'DE'),
            ('Florida', 'FL'), ('Georgia', 'GA'), ('Hawaii', 'HI'), ('Idaho', 'ID'),
            ('Illinois', 'IL'), ('Indiana', 'IN'), ('Iowa', 'IA'), ('Kansas', 'KS'),
            ('Kentucky', 'KY'), ('Louisiana', 'LA'), ('Maine', 'ME'), ('Maryland', 'MD'),
            ('Massachusetts', 'MA'), ('Michigan', 'MI'), ('Minnesota', 'MN'), ('Mississippi', 'MS'),
            ('Missouri', 'MO'), ('Montana', 'MT'), ('Nebraska', 'NE'), ('Nevada', 'NV'),
            ('New Hampshire', 'NH'), ('New Jersey', 'NJ'), ('New Mexico', 'NM'), ('New York', 'NY'),
            ('North Carolina', 'NC'), ('North Dakota', 'ND'), ('Ohio', 'OH'), ('Oklahoma', 'OK'),
            ('Oregon', 'OR'), ('Pennsylvania', 'PA'), ('Rhode Island', 'RI'), ('South Carolina', 'SC'),
            ('South Dakota', 'SD'), ('Tennessee', 'TN'), ('Texas', 'TX'), ('Utah', 'UT'),
            ('Vermont', 'VT'), ('Virginia', 'VA'), ('Washington', 'WA'), ('West Virginia', 'WV'),
            ('Wisconsin', 'WI'), ('Wyoming', 'WY'), ('District of Columbia', 'DC')
        ]
        
        schema = StructType([
            StructField("state", StringType(), True),
            StructField("state_abbreviation", StringType(), True)
        ])
        
        states_df = self.spark.createDataFrame(states_data, schema)
        states_df.write.mode("overwrite").saveAsTable(f"{self.database_name}.states_map")
        
        logger.info("Created states_map table")

    def transform_location(self) -> None:
        """
        Transform location table from Synthea patients data
        Equivalent to insert_location.sql
        """
        logger.info("Transforming location table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.location
        SELECT DISTINCT
            ROW_NUMBER() OVER (ORDER BY p.city, p.state, p.zip) as location_id,
            p.address,
            p.city,
            p.state,
            p.zip,
            NULL as county,
            p.state as location_source_value,
            NULL as country_concept_id,
            NULL as country_source_value,
            p.lat as latitude,
            p.lon as longitude
        FROM {self.database_name}.synthea_patients p
        WHERE p.city IS NOT NULL 
          AND p.state IS NOT NULL 
          AND p.zip IS NOT NULL
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed location transformation")

    def transform_care_site(self) -> None:
        """
        Transform care_site table from Synthea organizations data
        Equivalent to insert_care_site.sql
        """
        logger.info("Transforming care_site table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.care_site
        SELECT
            ROW_NUMBER() OVER (ORDER BY o.id) as care_site_id,
            o.name as care_site_name,
            32854 as place_of_service_concept_id,
            l.location_id,
            o.id as care_site_source_value,
            'Outpatient Clinic' as place_of_service_source_value
        FROM {self.database_name}.synthea_organizations o
        LEFT JOIN (
            SELECT DISTINCT l.location_id, l.city, l.state, l.zip
            FROM {self.database_name}.location l
            LEFT JOIN {self.database_name}.states_map sm 
                ON l.state = sm.state_abbreviation
        ) l ON o.city = l.city AND o.state = l.state AND o.zip = l.zip
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed care_site transformation")

    def transform_person(self) -> None:
        """
        Transform person table from Synthea patients data
        Equivalent to insert_person.sql
        """
        logger.info("Transforming person table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.person
        WITH mapped_states AS (
            SELECT DISTINCT 
                l.location_id,
                l.city,
                sm.state,
                l.zip
            FROM {self.database_name}.location l
            LEFT JOIN {self.database_name}.states_map sm 
                ON l.state = sm.state_abbreviation
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY p.id) as person_id,
            CASE UPPER(p.gender)
                WHEN 'M' THEN 8507
                WHEN 'F' THEN 8532
                ELSE 0
            END as gender_concept_id,
            YEAR(p.birthdate) as year_of_birth,
            MONTH(p.birthdate) as month_of_birth,
            DAY(p.birthdate) as day_of_birth,
            CAST(p.birthdate AS TIMESTAMP) as birth_datetime,
            CASE UPPER(p.race)
                WHEN 'WHITE' THEN 8527
                WHEN 'BLACK' THEN 8516
                WHEN 'ASIAN' THEN 8515
                ELSE 0
            END as race_concept_id,
            CASE UPPER(p.ethnicity)
                WHEN 'HISPANIC' THEN 38003563
                WHEN 'NONHISPANIC' THEN 38003564
                ELSE 0
            END as ethnicity_concept_id,
            l.location_id,
            NULL as provider_id,
            NULL as care_site_id,
            p.id as person_source_value,
            p.gender as gender_source_value,
            0 as gender_source_concept_id,
            p.race as race_source_value,
            0 as race_source_concept_id,
            p.ethnicity as ethnicity_source_value,
            0 as ethnicity_source_concept_id
        FROM {self.database_name}.synthea_patients p
        LEFT JOIN mapped_states l 
            ON p.city = l.city 
            AND p.state = l.state 
            AND p.zip = l.zip
        WHERE p.gender IS NOT NULL
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed person transformation")

    def transform_observation_period(self) -> None:
        """
        Transform observation_period table
        Equivalent to insert_observation_period.sql
        """
        logger.info("Transforming observation_period table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.observation_period
        SELECT
            ROW_NUMBER() OVER (ORDER BY person_id) as observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            32827 as period_type_concept_id
        FROM (
            SELECT
                p.person_id,
                p.birth_datetime as observation_period_start_date,
                COALESCE(
                    CAST(sp.deathdate AS DATE),
                    CURRENT_DATE()
                ) as observation_period_end_date
            FROM {self.database_name}.person p
            LEFT JOIN {self.database_name}.synthea_patients sp
                ON p.person_source_value = sp.id
        )
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed observation_period transformation")

    def transform_provider(self) -> None:
        """
        Transform provider table from Synthea providers data
        Equivalent to insert_provider.sql
        """
        logger.info("Transforming provider table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.provider
        SELECT
            ROW_NUMBER() OVER (ORDER BY pr.id) as provider_id,
            pr.name as provider_name,
            NULL as npi,
            NULL as dea,
            38004446 as specialty_concept_id,
            cs.care_site_id,
            YEAR(CURRENT_DATE()) as year_of_birth,
            8507 as gender_concept_id,
            pr.id as provider_source_value,
            pr.speciality as specialty_source_value,
            0 as specialty_source_concept_id,
            'M' as gender_source_value,
            0 as gender_source_concept_id
        FROM {self.database_name}.synthea_providers pr
        LEFT JOIN {self.database_name}.care_site cs
            ON pr.organization = cs.care_site_source_value
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed provider transformation")

    def transform_condition_occurrence(self, synthea_version: str) -> None:
        """
        Transform condition_occurrence table from Synthea conditions data
        Equivalent to insert_condition_occurrence.sql
        """
        logger.info("Transforming condition_occurrence table...")
        
        # Determine vocabulary filter based on synthea version
        vocab_filter = ""
        if synthea_version in ["2.7.0", "3.0.0", "3.1.0", "3.2.0"]:
            vocab_filter = "AND srctostdvm.source_vocabulary_id = 'SNOMED'"
        elif synthea_version == "3.3.0":
            vocab_filter = "AND srctostdvm.source_vocabulary_id IN ('SNOMED', 'ICD10CM')"
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.condition_occurrence
        SELECT
            ROW_NUMBER() OVER (ORDER BY p.person_id) as condition_occurrence_id,
            p.person_id,
            srctostdvm.target_concept_id as condition_concept_id,
            c.start as condition_start_date,
            CAST(c.start AS TIMESTAMP) as condition_start_datetime,
            c.stop as condition_end_date,
            CAST(c.stop AS TIMESTAMP) as condition_end_datetime,
            32827 as condition_type_concept_id,
            NULL as stop_reason,
            pr.provider_id,
            fv.visit_occurrence_id_new as visit_occurrence_id,
            fv.visit_occurrence_id_new + 1000000 as visit_detail_id,
            c.code as condition_source_value,
            srctosrcvm.source_concept_id as condition_source_concept_id,
            NULL as condition_status_source_value,
            0 as condition_status_concept_id
        FROM {self.database_name}.synthea_conditions c
        JOIN {self.database_name}.source_to_standard_vocab_map srctostdvm
            ON srctostdvm.source_code = c.code
            AND srctostdvm.target_domain_id = 'Condition'
            AND srctostdvm.target_vocabulary_id = 'SNOMED'
            {vocab_filter}
            AND srctostdvm.target_standard_concept = 'S'
            AND srctostdvm.target_invalid_reason IS NULL
        JOIN {self.database_name}.source_to_source_vocab_map srctosrcvm
            ON srctosrcvm.source_code = c.code
            {vocab_filter.replace('srctostdvm', 'srctosrcvm')}
            AND srctosrcvm.source_domain_id = 'Condition'
        LEFT JOIN {self.database_name}.final_visit_ids fv
            ON fv.encounter_id = c.encounter
        LEFT JOIN {self.database_name}.synthea_encounters e
            ON c.encounter = e.id
            AND c.patient = e.patient
        LEFT JOIN {self.database_name}.provider pr
            ON e.provider = pr.provider_source_value
        JOIN {self.database_name}.person p
            ON c.patient = p.person_source_value
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed condition_occurrence transformation")

    def transform_drug_exposure(self) -> None:
        """
        Transform drug_exposure table from Synthea medications and immunizations
        Equivalent to insert_drug_exposure.sql
        """
        logger.info("Transforming drug_exposure table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.drug_exposure
        SELECT 
            ROW_NUMBER() OVER (ORDER BY person_id) as drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_start_datetime,
            drug_exposure_end_date,
            drug_exposure_end_datetime,
            verbatim_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value
        FROM (
            -- Medications
            SELECT
                p.person_id,
                srctostdvm.target_concept_id as drug_concept_id,
                m.start as drug_exposure_start_date,
                CAST(m.start AS TIMESTAMP) as drug_exposure_start_datetime,
                COALESCE(m.stop, m.start) as drug_exposure_end_date,
                CAST(COALESCE(m.stop, m.start) AS TIMESTAMP) as drug_exposure_end_datetime,
                m.stop as verbatim_end_date,
                32838 as drug_type_concept_id,
                NULL as stop_reason,
                0 as refills,
                0 as quantity,
                COALESCE(DATEDIFF(m.stop, m.start), 0) as days_supply,
                NULL as sig,
                0 as route_concept_id,
                0 as lot_number,
                pr.provider_id,
                fv.visit_occurrence_id_new as visit_occurrence_id,
                fv.visit_occurrence_id_new + 1000000 as visit_detail_id,
                m.code as drug_source_value,
                srctosrcvm.source_concept_id as drug_source_concept_id,
                NULL as route_source_value,
                NULL as dose_unit_source_value
            FROM {self.database_name}.synthea_medications m
            JOIN {self.database_name}.source_to_standard_vocab_map srctostdvm
                ON srctostdvm.source_code = CAST(m.code AS STRING)
                AND srctostdvm.target_domain_id = 'Drug'
                AND srctostdvm.target_vocabulary_id = 'RxNorm'
                AND srctostdvm.target_standard_concept = 'S'
                AND srctostdvm.target_invalid_reason IS NULL
            JOIN {self.database_name}.source_to_source_vocab_map srctosrcvm
                ON srctosrcvm.source_code = CAST(m.code AS STRING)
                AND srctosrcvm.source_vocabulary_id = 'RxNorm'
            LEFT JOIN {self.database_name}.final_visit_ids fv
                ON fv.encounter_id = m.encounter
            LEFT JOIN {self.database_name}.synthea_encounters e
                ON m.encounter = e.id
                AND m.patient = e.patient
            LEFT JOIN {self.database_name}.provider pr
                ON e.provider = pr.provider_source_value
            JOIN {self.database_name}.person p
                ON p.person_source_value = m.patient
            
            UNION ALL
            
            -- Immunizations
            SELECT
                p.person_id,
                srctostdvm.target_concept_id as drug_concept_id,
                i.date as drug_exposure_start_date,
                CAST(i.date AS TIMESTAMP) as drug_exposure_start_datetime,
                i.date as drug_exposure_end_date,
                CAST(i.date AS TIMESTAMP) as drug_exposure_end_datetime,
                i.date as verbatim_end_date,
                32827 as drug_type_concept_id,
                NULL as stop_reason,
                0 as refills,
                0 as quantity,
                0 as days_supply,
                NULL as sig,
                0 as route_concept_id,
                0 as lot_number,
                pr.provider_id,
                fv.visit_occurrence_id_new as visit_occurrence_id,
                fv.visit_occurrence_id_new + 1000000 as visit_detail_id,
                i.code as drug_source_value,
                srctosrcvm.source_concept_id as drug_source_concept_id,
                NULL as route_source_value,
                NULL as dose_unit_source_value
            FROM {self.database_name}.synthea_immunizations i
            JOIN {self.database_name}.source_to_standard_vocab_map srctostdvm
                ON srctostdvm.source_code = CAST(i.code AS STRING)
                AND srctostdvm.target_domain_id = 'Drug'
                AND srctostdvm.target_vocabulary_id = 'CVX'
                AND srctostdvm.target_standard_concept = 'S'
                AND srctostdvm.target_invalid_reason IS NULL
            JOIN {self.database_name}.source_to_source_vocab_map srctosrcvm
                ON srctosrcvm.source_code = CAST(i.code AS STRING)
                AND srctosrcvm.source_vocabulary_id = 'CVX'
            LEFT JOIN {self.database_name}.final_visit_ids fv
                ON fv.encounter_id = i.encounter
            LEFT JOIN {self.database_name}.synthea_encounters e
                ON i.encounter = e.id
                AND i.patient = e.patient
            LEFT JOIN {self.database_name}.provider pr
                ON e.provider = pr.provider_source_value
            JOIN {self.database_name}.person p
                ON p.person_source_value = i.patient
        )
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed drug_exposure transformation")

    def create_visit_rollup_tables(self) -> None:
        """
        Create visit rollup tables for encounter aggregation
        Equivalent to CreateVisitRollupTables.r logic
        """
        logger.info("Creating visit rollup tables...")
        
        # Create all_visits table
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.all_visits")
        
        all_visits_sql = f"""
        CREATE TABLE {self.database_name}.all_visits AS
        SELECT
            e.id as encounter_id,
            e.patient,
            e.start as visit_start_date,
            e.stop as visit_end_date,
            e.encounterclass,
            ROW_NUMBER() OVER (ORDER BY e.patient, e.start) as visit_occurrence_id
        FROM {self.database_name}.synthea_encounters e
        WHERE e.start IS NOT NULL
        """
        
        self.spark.sql(all_visits_sql)
        
        # Create final_visit_ids table
        self.spark.sql(f"DROP TABLE IF EXISTS {self.database_name}.final_visit_ids")
        
        final_visit_ids_sql = f"""
        CREATE TABLE {self.database_name}.final_visit_ids AS
        SELECT
            encounter_id,
            visit_occurrence_id as visit_occurrence_id_new
        FROM {self.database_name}.all_visits
        """
        
        self.spark.sql(final_visit_ids_sql)
        
        logger.info("Created visit rollup tables")

    def transform_visit_occurrence(self) -> None:
        """
        Transform visit_occurrence table
        Equivalent to insert_visit_occurrence.sql
        """
        logger.info("Transforming visit_occurrence table...")
        
        transform_sql = f"""
        INSERT OVERWRITE TABLE {self.database_name}.visit_occurrence
        SELECT
            av.visit_occurrence_id,
            p.person_id,
            CASE LOWER(av.encounterclass)
                WHEN 'ambulatory' THEN 9202
                WHEN 'emergency' THEN 9203
                WHEN 'inpatient' THEN 9201
                WHEN 'wellness' THEN 9202
                WHEN 'urgentcare' THEN 9203
                WHEN 'outpatient' THEN 9202
                ELSE 0
            END as visit_concept_id,
            av.visit_start_date,
            CAST(av.visit_start_date AS TIMESTAMP) as visit_start_datetime,
            av.visit_end_date,
            CAST(av.visit_end_date AS TIMESTAMP) as visit_end_datetime,
            32827 as visit_type_concept_id,
            pr.provider_id,
            NULL as care_site_id,
            av.encounter_id as visit_source_value,
            0 as visit_source_concept_id,
            0 as admitted_from_concept_id,
            NULL as admitted_from_source_value,
            0 as discharged_to_concept_id,
            NULL as discharged_to_source_value,
            LAG(av.visit_occurrence_id) OVER (
                PARTITION BY p.person_id 
                ORDER BY av.visit_start_date
            ) as preceding_visit_occurrence_id
        FROM {self.database_name}.all_visits av
        JOIN {self.database_name}.person p
            ON av.patient = p.person_source_value
        JOIN {self.database_name}.synthea_encounters e
            ON av.encounter_id = e.id
            AND av.patient = e.patient
        LEFT JOIN {self.database_name}.provider pr
            ON e.provider = pr.provider_source_value
        WHERE av.visit_occurrence_id IN (
            SELECT DISTINCT visit_occurrence_id_new
            FROM {self.database_name}.final_visit_ids
        )
        """
        
        self.spark.sql(transform_sql)
        logger.info("Completed visit_occurrence transformation")

    def get_event_concept_id(self, source_code: str, target_domain_id: str, target_vocabulary_id: str) -> int:
        """
        Get concept ID for a source code - utility function
        Equivalent to get_event_concept_id.sql
        """
        concept_query = f"""
        SELECT target_concept_id
        FROM {self.database_name}.source_to_standard_vocab_map
        WHERE source_code = '{source_code}'
          AND target_domain_id = '{target_domain_id}'
          AND target_vocabulary_id = '{target_vocabulary_id}'
          AND target_standard_concept = 'S'
          AND target_invalid_reason IS NULL
        LIMIT 1
        """
        
        result = self.spark.sql(concept_query).collect()
        return result[0]['target_concept_id'] if result else 0

    # Additional transformation methods would be implemented here following the same pattern
    # Each method corresponds to an insert_*.sql file in the original R package
    
    def transform_measurement(self, synthea_version: str) -> None:
        """Transform measurement table - placeholder"""
        logger.info("Transforming measurement table...")
        # Implementation would follow insert_measurement.sql logic
        pass
    
    def transform_procedure_occurrence(self, synthea_version: str) -> None:
        """Transform procedure_occurrence table - placeholder"""
        logger.info("Transforming procedure_occurrence table...")
        # Implementation would follow insert_procedure_occurrence.sql logic
        pass
    
    def transform_observation(self) -> None:
        """Transform observation table - placeholder"""
        logger.info("Transforming observation table...")
        # Implementation would follow insert_observation.sql logic
        pass
    
    def transform_visit_detail(self) -> None:
        """Transform visit_detail table - placeholder"""
        logger.info("Transforming visit_detail table...")
        # Implementation would follow insert_visit_detail.sql logic
        pass
    
    def transform_condition_era(self) -> None:
        """Transform condition_era table - placeholder"""
        logger.info("Transforming condition_era table...")
        # Implementation would follow insert_condition_era.sql logic
        pass
    
    def transform_drug_era(self) -> None:
        """Transform drug_era table - placeholder"""
        logger.info("Transforming drug_era table...")
        # Implementation would follow insert_drug_era.sql logic
        pass
    
    def transform_cdm_source(self, cdm_version: str, cdm_source_name: str, 
                           cdm_source_abbreviation: str, cdm_holder: str, 
                           cdm_source_description: str, synthea_version: str) -> None:
        """Transform cdm_source table - placeholder"""
        logger.info("Transforming cdm_source table...")
        # Implementation would follow insert_cdm_source.sql logic
        pass
    
    def transform_device_exposure(self) -> None:
        """Transform device_exposure table - placeholder"""
        logger.info("Transforming device_exposure table...")
        # Implementation would follow insert_device_exposure.sql logic
        pass
    
    def transform_death(self) -> None:
        """Transform death table - placeholder"""
        logger.info("Transforming death table...")
        # Implementation would follow insert_death.sql logic
        pass
    
    def transform_payer_plan_period(self, synthea_version: str) -> None:
        """Transform payer_plan_period table - placeholder"""
        logger.info("Transforming payer_plan_period table...")
        # Implementation would follow insert_payer_plan_period.sql logic
        pass
    
    def transform_cost(self, synthea_version: str) -> None:
        """Transform cost table - placeholder"""
        logger.info("Transforming cost table...")
        # Implementation would follow insert_cost.sql logic
        pass
