#!/usr/bin/env python3
"""
OMOP CDM Table Schemas - PySpark Implementation
==============================================

This module contains the complete OMOP CDM table schema definitions for PySpark.
Schemas are based on CDM v5.3 and v5.4 specifications.
"""

from pyspark.sql.types import *
from typing import Dict

def get_cdm_table_schemas(cdm_version: str = "5.4") -> Dict[str, StructType]:
    """
    Get complete OMOP CDM table schemas for the specified version
    
    Args:
        cdm_version: CDM version ("5.3" or "5.4")
        
    Returns:
        Dictionary of table names to StructType schemas
    """
    
    if cdm_version not in ["5.3", "5.4"]:
        raise ValueError("Unsupported CDM version. Supported versions are '5.3' and '5.4'")
    
    schemas = {}
    
    # PERSON table
    schemas['person'] = StructType([
        StructField('person_id', LongType(), False),
        StructField('gender_concept_id', IntegerType(), False),
        StructField('year_of_birth', IntegerType(), False),
        StructField('month_of_birth', IntegerType(), True),
        StructField('day_of_birth', IntegerType(), True),
        StructField('birth_datetime', TimestampType(), True),
        StructField('race_concept_id', IntegerType(), False),
        StructField('ethnicity_concept_id', IntegerType(), False),
        StructField('location_id', LongType(), True),
        StructField('provider_id', LongType(), True),
        StructField('care_site_id', LongType(), True),
        StructField('person_source_value', StringType(), True),
        StructField('gender_source_value', StringType(), True),
        StructField('gender_source_concept_id', IntegerType(), True),
        StructField('race_source_value', StringType(), True),
        StructField('race_source_concept_id', IntegerType(), True),
        StructField('ethnicity_source_value', StringType(), True),
        StructField('ethnicity_source_concept_id', IntegerType(), True)
    ])
    
    # OBSERVATION_PERIOD table
    schemas['observation_period'] = StructType([
        StructField('observation_period_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('observation_period_start_date', DateType(), False),
        StructField('observation_period_end_date', DateType(), False),
        StructField('period_type_concept_id', IntegerType(), False)
    ])
    
    # VISIT_OCCURRENCE table
    schemas['visit_occurrence'] = StructType([
        StructField('visit_occurrence_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('visit_concept_id', IntegerType(), False),
        StructField('visit_start_date', DateType(), False),
        StructField('visit_start_datetime', TimestampType(), True),
        StructField('visit_end_date', DateType(), False),
        StructField('visit_end_datetime', TimestampType(), True),
        StructField('visit_type_concept_id', IntegerType(), False),
        StructField('provider_id', LongType(), True),
        StructField('care_site_id', LongType(), True),
        StructField('visit_source_value', StringType(), True),
        StructField('visit_source_concept_id', IntegerType(), True),
        StructField('admitted_from_concept_id', IntegerType(), True),
        StructField('admitted_from_source_value', StringType(), True),
        StructField('discharged_to_concept_id', IntegerType(), True),
        StructField('discharged_to_source_value', StringType(), True),
        StructField('preceding_visit_occurrence_id', LongType(), True)
    ])
    
    # VISIT_DETAIL table
    schemas['visit_detail'] = StructType([
        StructField('visit_detail_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('visit_detail_concept_id', IntegerType(), False),
        StructField('visit_detail_start_date', DateType(), False),
        StructField('visit_detail_start_datetime', TimestampType(), True),
        StructField('visit_detail_end_date', DateType(), False),
        StructField('visit_detail_end_datetime', TimestampType(), True),
        StructField('visit_detail_type_concept_id', IntegerType(), False),
        StructField('provider_id', LongType(), True),
        StructField('care_site_id', LongType(), True),
        StructField('visit_detail_source_value', StringType(), True),
        StructField('visit_detail_source_concept_id', IntegerType(), True),
        StructField('admitted_from_concept_id', IntegerType(), True),
        StructField('admitted_from_source_value', StringType(), True),
        StructField('discharged_to_concept_id', IntegerType(), True),
        StructField('discharged_to_source_value', StringType(), True),
        StructField('preceding_visit_detail_id', LongType(), True),
        StructField('parent_visit_detail_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), False)
    ])
    
    # CONDITION_OCCURRENCE table
    schemas['condition_occurrence'] = StructType([
        StructField('condition_occurrence_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('condition_concept_id', IntegerType(), False),
        StructField('condition_start_date', DateType(), False),
        StructField('condition_start_datetime', TimestampType(), True),
        StructField('condition_end_date', DateType(), True),
        StructField('condition_end_datetime', TimestampType(), True),
        StructField('condition_type_concept_id', IntegerType(), False),
        StructField('condition_status_concept_id', IntegerType(), True),
        StructField('stop_reason', StringType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('condition_source_value', StringType(), True),
        StructField('condition_source_concept_id', IntegerType(), True),
        StructField('condition_status_source_value', StringType(), True)
    ])
    
    # DRUG_EXPOSURE table
    schemas['drug_exposure'] = StructType([
        StructField('drug_exposure_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('drug_concept_id', IntegerType(), False),
        StructField('drug_exposure_start_date', DateType(), False),
        StructField('drug_exposure_start_datetime', TimestampType(), True),
        StructField('drug_exposure_end_date', DateType(), False),
        StructField('drug_exposure_end_datetime', TimestampType(), True),
        StructField('verbatim_end_date', DateType(), True),
        StructField('drug_type_concept_id', IntegerType(), False),
        StructField('stop_reason', StringType(), True),
        StructField('refills', IntegerType(), True),
        StructField('quantity', DoubleType(), True),
        StructField('days_supply', IntegerType(), True),
        StructField('sig', StringType(), True),
        StructField('route_concept_id', IntegerType(), True),
        StructField('lot_number', StringType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('drug_source_value', StringType(), True),
        StructField('drug_source_concept_id', IntegerType(), True),
        StructField('route_source_value', StringType(), True),
        StructField('dose_unit_source_value', StringType(), True)
    ])
    
    # PROCEDURE_OCCURRENCE table
    schemas['procedure_occurrence'] = StructType([
        StructField('procedure_occurrence_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('procedure_concept_id', IntegerType(), False),
        StructField('procedure_date', DateType(), False),
        StructField('procedure_datetime', TimestampType(), True),
        StructField('procedure_type_concept_id', IntegerType(), False),
        StructField('modifier_concept_id', IntegerType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('procedure_source_value', StringType(), True),
        StructField('procedure_source_concept_id', IntegerType(), True),
        StructField('modifier_source_value', StringType(), True)
    ])
    
    # DEVICE_EXPOSURE table
    schemas['device_exposure'] = StructType([
        StructField('device_exposure_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('device_concept_id', IntegerType(), False),
        StructField('device_exposure_start_date', DateType(), False),
        StructField('device_exposure_start_datetime', TimestampType(), True),
        StructField('device_exposure_end_date', DateType(), True),
        StructField('device_exposure_end_datetime', TimestampType(), True),
        StructField('device_type_concept_id', IntegerType(), False),
        StructField('unique_device_id', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('device_source_value', StringType(), True),
        StructField('device_source_concept_id', IntegerType(), True),
        StructField('unit_concept_id', IntegerType(), True),
        StructField('unit_source_value', StringType(), True),
        StructField('unit_source_concept_id', IntegerType(), True)
    ])
    
    # MEASUREMENT table
    schemas['measurement'] = StructType([
        StructField('measurement_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('measurement_concept_id', IntegerType(), False),
        StructField('measurement_date', DateType(), False),
        StructField('measurement_datetime', TimestampType(), True),
        StructField('measurement_time', StringType(), True),
        StructField('measurement_type_concept_id', IntegerType(), False),
        StructField('operator_concept_id', IntegerType(), True),
        StructField('value_as_number', DoubleType(), True),
        StructField('value_as_concept_id', IntegerType(), True),
        StructField('unit_concept_id', IntegerType(), True),
        StructField('range_low', DoubleType(), True),
        StructField('range_high', DoubleType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('measurement_source_value', StringType(), True),
        StructField('measurement_source_concept_id', IntegerType(), True),
        StructField('unit_source_value', StringType(), True),
        StructField('unit_source_concept_id', IntegerType(), True),
        StructField('value_source_value', StringType(), True),
        StructField('measurement_event_id', LongType(), True),
        StructField('meas_event_field_concept_id', IntegerType(), True)
    ])
    
    # OBSERVATION table
    schemas['observation'] = StructType([
        StructField('observation_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('observation_concept_id', IntegerType(), False),
        StructField('observation_date', DateType(), False),
        StructField('observation_datetime', TimestampType(), True),
        StructField('observation_type_concept_id', IntegerType(), False),
        StructField('value_as_number', DoubleType(), True),
        StructField('value_as_string', StringType(), True),
        StructField('value_as_concept_id', IntegerType(), True),
        StructField('qualifier_concept_id', IntegerType(), True),
        StructField('unit_concept_id', IntegerType(), True),
        StructField('provider_id', LongType(), True),
        StructField('visit_occurrence_id', LongType(), True),
        StructField('visit_detail_id', LongType(), True),
        StructField('observation_source_value', StringType(), True),
        StructField('observation_source_concept_id', IntegerType(), True),
        StructField('unit_source_value', StringType(), True),
        StructField('qualifier_source_value', StringType(), True),
        StructField('value_source_value', StringType(), True),
        StructField('observation_event_id', LongType(), True),
        StructField('obs_event_field_concept_id', IntegerType(), True)
    ])
    
    # DEATH table
    schemas['death'] = StructType([
        StructField('person_id', LongType(), False),
        StructField('death_date', DateType(), False),
        StructField('death_datetime', TimestampType(), True),
        StructField('death_type_concept_id', IntegerType(), False),
        StructField('cause_concept_id', IntegerType(), True),
        StructField('cause_source_value', StringType(), True),
        StructField('cause_source_concept_id', IntegerType(), True)
    ])
    
    # NOTE_NLP table (if cdm_version == "5.4")
    if cdm_version == "5.4":
        schemas['note_nlp'] = StructType([
            StructField('note_nlp_id', LongType(), False),
            StructField('note_id', LongType(), False),
            StructField('section_concept_id', IntegerType(), True),
            StructField('snippet', StringType(), True),
            StructField('offset', StringType(), True),
            StructField('lexical_variant', StringType(), False),
            StructField('note_nlp_concept_id', IntegerType(), True),
            StructField('note_nlp_source_concept_id', IntegerType(), True),
            StructField('nlp_system', StringType(), True),
            StructField('nlp_date', DateType(), False),
            StructField('nlp_datetime', TimestampType(), True),
            StructField('term_exists', StringType(), True),
            StructField('term_temporal', StringType(), True),
            StructField('term_modifiers', StringType(), True)
        ])
    
    # SPECIMEN table
    schemas['specimen'] = StructType([
        StructField('specimen_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('specimen_concept_id', IntegerType(), False),
        StructField('specimen_type_concept_id', IntegerType(), False),
        StructField('specimen_date', DateType(), False),
        StructField('specimen_datetime', TimestampType(), True),
        StructField('quantity', DoubleType(), True),
        StructField('unit_concept_id', IntegerType(), True),
        StructField('anatomic_site_concept_id', IntegerType(), True),
        StructField('disease_status_concept_id', IntegerType(), True),
        StructField('specimen_source_id', StringType(), True),
        StructField('specimen_source_value', StringType(), True),
        StructField('unit_source_value', StringType(), True),
        StructField('anatomic_site_source_value', StringType(), True),
        StructField('disease_status_source_value', StringType(), True)
    ])
    
    # LOCATION table
    schemas['location'] = StructType([
        StructField('location_id', LongType(), False),
        StructField('address_1', StringType(), True),
        StructField('address_2', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('zip', StringType(), True),
        StructField('county', StringType(), True),
        StructField('location_source_value', StringType(), True),
        StructField('country_concept_id', IntegerType(), True),
        StructField('country_source_value', StringType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True)
    ])
    
    # CARE_SITE table
    schemas['care_site'] = StructType([
        StructField('care_site_id', LongType(), False),
        StructField('care_site_name', StringType(), True),
        StructField('place_of_service_concept_id', IntegerType(), True),
        StructField('location_id', LongType(), True),
        StructField('care_site_source_value', StringType(), True),
        StructField('place_of_service_source_value', StringType(), True)
    ])
    
    # PROVIDER table
    schemas['provider'] = StructType([
        StructField('provider_id', LongType(), False),
        StructField('provider_name', StringType(), True),
        StructField('npi', StringType(), True),
        StructField('dea', StringType(), True),
        StructField('specialty_concept_id', IntegerType(), True),
        StructField('care_site_id', LongType(), True),
        StructField('year_of_birth', IntegerType(), True),
        StructField('gender_concept_id', IntegerType(), True),
        StructField('provider_source_value', StringType(), True),
        StructField('specialty_source_value', StringType(), True),
        StructField('specialty_source_concept_id', IntegerType(), True),
        StructField('gender_source_value', StringType(), True),
        StructField('gender_source_concept_id', IntegerType(), True)
    ])
    
    # PAYER_PLAN_PERIOD table
    schemas['payer_plan_period'] = StructType([
        StructField('payer_plan_period_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('payer_plan_period_start_date', DateType(), False),
        StructField('payer_plan_period_end_date', DateType(), False),
        StructField('payer_concept_id', IntegerType(), True),
        StructField('payer_source_value', StringType(), True),
        StructField('payer_source_concept_id', IntegerType(), True),
        StructField('plan_concept_id', IntegerType(), True),
        StructField('plan_source_value', StringType(), True),
        StructField('plan_source_concept_id', IntegerType(), True),
        StructField('sponsor_concept_id', IntegerType(), True),
        StructField('sponsor_source_value', StringType(), True),
        StructField('sponsor_source_concept_id', IntegerType(), True),
        StructField('family_source_value', StringType(), True),
        StructField('stop_reason_concept_id', IntegerType(), True),
        StructField('stop_reason_source_value', StringType(), True),
        StructField('stop_reason_source_concept_id', IntegerType(), True)
    ])
    
    # COST table
    schemas['cost'] = StructType([
        StructField('cost_id', LongType(), False),
        StructField('cost_event_id', LongType(), False),
        StructField('cost_domain_id', StringType(), False),
        StructField('cost_type_concept_id', IntegerType(), False),
        StructField('currency_concept_id', IntegerType(), True),
        StructField('total_charge', DoubleType(), True),
        StructField('total_cost', DoubleType(), True),
        StructField('total_paid', DoubleType(), True),
        StructField('paid_by_payer', DoubleType(), True),
        StructField('paid_by_patient', DoubleType(), True),
        StructField('paid_patient_copay', DoubleType(), True),
        StructField('paid_patient_coinsurance', DoubleType(), True),
        StructField('paid_patient_deductible', DoubleType(), True),
        StructField('paid_by_primary', DoubleType(), True),
        StructField('paid_ingredient_cost', DoubleType(), True),
        StructField('paid_dispensing_fee', DoubleType(), True),
        StructField('payer_plan_period_id', LongType(), True),
        StructField('amount_allowed', DoubleType(), True),
        StructField('revenue_code_concept_id', IntegerType(), True),
        StructField('revenue_code_source_value', StringType(), True),
        StructField('drg_concept_id', IntegerType(), True),
        StructField('drg_source_value', StringType(), True)
    ])
    
    # ERA tables
    schemas['drug_era'] = StructType([
        StructField('drug_era_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('drug_concept_id', IntegerType(), False),
        StructField('drug_era_start_date', DateType(), False),
        StructField('drug_era_end_date', DateType(), False),
        StructField('drug_exposure_count', IntegerType(), True),
        StructField('gap_days', IntegerType(), True)
    ])
    
    schemas['dose_era'] = StructType([
        StructField('dose_era_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('drug_concept_id', IntegerType(), False),
        StructField('unit_concept_id', IntegerType(), False),
        StructField('dose_value', DoubleType(), False),
        StructField('dose_era_start_date', DateType(), False),
        StructField('dose_era_end_date', DateType(), False)
    ])
    
    schemas['condition_era'] = StructType([
        StructField('condition_era_id', LongType(), False),
        StructField('person_id', LongType(), False),
        StructField('condition_concept_id', IntegerType(), False),
        StructField('condition_era_start_date', DateType(), False),
        StructField('condition_era_end_date', DateType(), False),
        StructField('condition_occurrence_count', IntegerType(), True)
    ])
    
    # METADATA tables
    schemas['cdm_source'] = StructType([
        StructField('cdm_source_name', StringType(), False),
        StructField('cdm_source_abbreviation', StringType(), False),
        StructField('cdm_holder', StringType(), False),
        StructField('source_description', StringType(), True),
        StructField('source_documentation_reference', StringType(), True),
        StructField('cdm_etl_reference', StringType(), True),
        StructField('source_release_date', DateType(), False),
        StructField('cdm_release_date', DateType(), False),
        StructField('cdm_version', StringType(), True),
        StructField('cdm_version_concept_id', IntegerType(), False),
        StructField('vocabulary_version', StringType(), False)
    ])
    
    schemas['metadata'] = StructType([
        StructField('metadata_id', IntegerType(), False),
        StructField('metadata_concept_id', IntegerType(), False),
        StructField('metadata_type_concept_id', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('value_as_string', StringType(), True),
        StructField('value_as_concept_id', IntegerType(), True),
        StructField('value_as_number', DoubleType(), True),
        StructField('metadata_date', DateType(), True),
        StructField('metadata_datetime', TimestampType(), True)
    ])
    
    return schemas

def get_synthea_table_schemas(synthea_version: str = "2.7.0") -> Dict[str, StructType]:
    """
    Get Synthea table schemas for the specified version
    
    Args:
        synthea_version: Synthea version
        
    Returns:
        Dictionary of table names to StructType schemas
    """
    
    schemas = {}
    
    # Patients table
    schemas['patients'] = StructType([
        StructField('Id', StringType(), True),
        StructField('BIRTHDATE', DateType(), True),
        StructField('DEATHDATE', DateType(), True),
        StructField('SSN', StringType(), True),
        StructField('DRIVERS', StringType(), True),
        StructField('PASSPORT', StringType(), True),
        StructField('PREFIX', StringType(), True),
        StructField('FIRST', StringType(), True),
        StructField('LAST', StringType(), True),
        StructField('SUFFIX', StringType(), True),
        StructField('MAIDEN', StringType(), True),
        StructField('MARITAL', StringType(), True),
        StructField('RACE', StringType(), True),
        StructField('ETHNICITY', StringType(), True),
        StructField('GENDER', StringType(), True),
        StructField('BIRTHPLACE', StringType(), True),
        StructField('ADDRESS', StringType(), True),
        StructField('CITY', StringType(), True),
        StructField('STATE', StringType(), True),
        StructField('COUNTY', StringType(), True),
        StructField('ZIP', StringType(), True),
        StructField('LAT', DoubleType(), True),
        StructField('LON', DoubleType(), True),
        StructField('HEALTHCARE_EXPENSES', DoubleType(), True),
        StructField('HEALTHCARE_COVERAGE', DoubleType(), True)
    ])
    
    # Encounters table
    schemas['encounters'] = StructType([
        StructField('Id', StringType(), True),
        StructField('START', TimestampType(), True),
        StructField('STOP', TimestampType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ORGANIZATION', StringType(), True),
        StructField('PROVIDER', StringType(), True),
        StructField('PAYER', StringType(), True),
        StructField('ENCOUNTERCLASS', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('BASE_ENCOUNTER_COST', DoubleType(), True),
        StructField('TOTAL_CLAIM_COST', DoubleType(), True),
        StructField('PAYER_COVERAGE', DoubleType(), True),
        StructField('REASONCODE', StringType(), True),
        StructField('REASONDESCRIPTION', StringType(), True)
    ])
    
    # Conditions table
    schemas['conditions'] = StructType([
        StructField('START', DateType(), True),
        StructField('STOP', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True)
    ])
    
    # Medications table
    schemas['medications'] = StructType([
        StructField('START', DateType(), True),
        StructField('STOP', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('PAYER', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('BASE_COST', DoubleType(), True),
        StructField('PAYER_COVERAGE', DoubleType(), True),
        StructField('DISPENSES', IntegerType(), True),
        StructField('TOTALCOST', DoubleType(), True),
        StructField('REASONCODE', StringType(), True),
        StructField('REASONDESCRIPTION', StringType(), True)
    ])
    
    # Observations table
    schemas['observations'] = StructType([
        StructField('DATE', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CATEGORY', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('VALUE', StringType(), True),
        StructField('UNITS', StringType(), True),
        StructField('TYPE', StringType(), True)
    ])
    
    # Procedures table
    schemas['procedures'] = StructType([
        StructField('START', DateType(), True),
        StructField('STOP', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('BASE_COST', DoubleType(), True),
        StructField('REASONCODE', StringType(), True),
        StructField('REASONDESCRIPTION', StringType(), True)
    ])
    
    # Immunizations table
    schemas['immunizations'] = StructType([
        StructField('DATE', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('BASE_COST', DoubleType(), True)
    ])
    
    # Allergies table
    schemas['allergies'] = StructType([
        StructField('START', DateType(), True),
        StructField('STOP', DateType(), True),
        StructField('PATIENT', StringType(), True),
        StructField('ENCOUNTER', StringType(), True),
        StructField('CODE', StringType(), True),
        StructField('SYSTEM', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('TYPE', StringType(), True),
        StructField('CATEGORY', StringType(), True),
        StructField('REACTION1', StringType(), True),
        StructField('DESCRIPTION1', StringType(), True),
        StructField('SEVERITY1', StringType(), True),
        StructField('REACTION2', StringType(), True),
        StructField('DESCRIPTION2', StringType(), True),
        StructField('SEVERITY2', StringType(), True)
    ])
    
    # Organizations table
    schemas['organizations'] = StructType([
        StructField('Id', StringType(), True),
        StructField('NAME', StringType(), True),
        StructField('ADDRESS', StringType(), True),
        StructField('CITY', StringType(), True),
        StructField('STATE', StringType(), True),
        StructField('ZIP', StringType(), True),
        StructField('LAT', DoubleType(), True),
        StructField('LON', DoubleType(), True),
        StructField('PHONE', StringType(), True),
        StructField('REVENUE', DoubleType(), True),
        StructField('UTILIZATION', IntegerType(), True)
    ])
    
    # Providers table
    schemas['providers'] = StructType([
        StructField('Id', StringType(), True),
        StructField('ORGANIZATION', StringType(), True),
        StructField('NAME', StringType(), True),
        StructField('GENDER', StringType(), True),
        StructField('SPECIALITY', StringType(), True),
        StructField('ADDRESS', StringType(), True),
        StructField('CITY', StringType(), True),
        StructField('STATE', StringType(), True),
        StructField('ZIP', StringType(), True),
        StructField('LAT', DoubleType(), True),
        StructField('LON', DoubleType(), True),
        StructField('ENCOUNTERS', IntegerType(), True),
        StructField('PROCEDURES', IntegerType(), True)
    ])
    
    # Payers table
    schemas['payers'] = StructType([
        StructField('Id', StringType(), True),
        StructField('NAME', StringType(), True),
        StructField('ADDRESS', StringType(), True),
        StructField('CITY', StringType(), True),
        StructField('STATE_HEADQUARTERED', StringType(), True),
        StructField('ZIP', StringType(), True),
        StructField('PHONE', StringType(), True),
        StructField('AMOUNT_COVERED', DoubleType(), True),
        StructField('AMOUNT_UNCOVERED', DoubleType(), True),
        StructField('REVENUE', DoubleType(), True),
        StructField('COVERED_ENCOUNTERS', IntegerType(), True),
        StructField('UNCOVERED_ENCOUNTERS', IntegerType(), True),
        StructField('COVERED_MEDICATIONS', IntegerType(), True),
        StructField('UNCOVERED_MEDICATIONS', IntegerType(), True),
        StructField('COVERED_PROCEDURES', IntegerType(), True),
        StructField('UNCOVERED_PROCEDURES', IntegerType(), True),
        StructField('COVERED_IMMUNIZATIONS', IntegerType(), True),
        StructField('UNCOVERED_IMMUNIZATIONS', IntegerType(), True),
        StructField('UNIQUE_CUSTOMERS', IntegerType(), True),
        StructField('QOLS_AVG', DoubleType(), True),
        StructField('MEMBER_MONTHS', IntegerType(), True)
    ])
    
    # Payer_transitions table
    schemas['payer_transitions'] = StructType([
        StructField('PATIENT', StringType(), True),
        StructField('MEMBERID', StringType(), True),
        StructField('START_DATE', DateType(), True),
        StructField('END_DATE', DateType(), True),
        StructField('PAYER', StringType(), True),
        StructField('SECONDARY_PAYER', StringType(), True),
        StructField('PLAN_OWNERSHIP', StringType(), True),
        StructField('OWNER_NAME', StringType(), True)
    ])
    
    # Devices table (if synthea_version >= "3.0.0")
    if synthea_version >= "3.0.0":
        schemas['devices'] = StructType([
            StructField('START', DateType(), True),
            StructField('STOP', DateType(), True),
            StructField('PATIENT', StringType(), True),
            StructField('ENCOUNTER', StringType(), True),
            StructField('CODE', StringType(), True),
            StructField('DESCRIPTION', StringType(), True),
            StructField('UDI', StringType(), True)
        ])
    
    return schemas
