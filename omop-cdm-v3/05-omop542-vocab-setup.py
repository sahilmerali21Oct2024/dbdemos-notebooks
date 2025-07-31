# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP Vocabulary Setup
# MAGIC Construct vocabulary tables, based on tables downloaded from [Athena](https://athena.ohdsi.org/search-terms/start) website and available here on `s3://hls-eng-data-public/data/rwe/omop-vocabs/`
# MAGIC If you like to download a different dataset, downoad the vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) and
# MAGIC use [databricks dbfs api](https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfs-api) utilities to upload downloaded vocabularies to `dbfs` under your `vocab_path`.
# MAGIC
# MAGIC <img align="right" width="700"  src="https://drive.google.com/uc?export=view&id=16TU2l7XHjQLugmS_McXegBXKMglD--Fr">

# COMMAND ----------

dbutils.widgets.text(name = "catalog_name", defaultValue="hls_omop", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="vocab_542", label="Schema Name")
# dbutils.widgets.text(name = "volume_name", defaultValue="omop_vocab", label="Volume Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get(name = "catalog_name")
schema_name = dbutils.widgets.get(name = "schema_name")
# volume_name = dbutils.widgets.get(name = "volume_name")
# vocab_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')
spark.sql(f'USE CATALOG {catalog_name}')

# COMMAND ----------

spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
spark.sql(f'USE SCHEMA {schema_name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE CONCEPT (
# MAGIC   CONCEPT_ID LONG,
# MAGIC   CONCEPT_NAME STRING,
# MAGIC   DOMAIN_ID STRING,
# MAGIC   VOCABULARY_ID STRING,
# MAGIC   CONCEPT_CLASS_ID STRING,
# MAGIC   STANDARD_CONCEPT STRING,
# MAGIC   CONCEPT_CODE STRING,
# MAGIC   VALID_START_DATE DATE,
# MAGIC   VALID_END_DATE DATE,
# MAGIC   INVALID_REASON STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE CONCEPT_ANCESTOR (
# MAGIC   ANCESTOR_CONCEPT_ID LONG,
# MAGIC   DESCENDANT_CONCEPT_ID LONG,
# MAGIC   MIN_LEVELS_OF_SEPARATION LONG,
# MAGIC   MAX_LEVELS_OF_SEPARATION LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE CONCEPT_CLASS (
# MAGIC   CONCEPT_CLASS_ID STRING,
# MAGIC   CONCEPT_CLASS_NAME STRING,
# MAGIC   CONCEPT_CLASS_CONCEPT_ID LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE CONCEPT_RELATIONSHIP (
# MAGIC   CONCEPT_ID_1 LONG,
# MAGIC   CONCEPT_ID_2 LONG,
# MAGIC   RELATIONSHIP_ID STRING,
# MAGIC   VALID_START_DATE DATE,
# MAGIC   VALID_END_DATE DATE,
# MAGIC   INVALID_REASON STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE CONCEPT_SYNONYM (
# MAGIC   CONCEPT_ID LONG,
# MAGIC   CONCEPT_SYNONYM_NAME STRING,
# MAGIC   LANGUAGE_CONCEPT_ID LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE DOMAIN (
# MAGIC   DOMAIN_ID STRING,
# MAGIC   DOMAIN_NAME STRING,
# MAGIC   DOMAIN_CONCEPT_ID LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE DRUG_STRENGTH (
# MAGIC   DRUG_CONCEPT_ID LONG,
# MAGIC   INGREDIENT_CONCEPT_ID LONG,
# MAGIC   AMOUNT_VALUE DOUBLE,
# MAGIC   AMOUNT_UNIT_CONCEPT_ID LONG,
# MAGIC   NUMERATOR_VALUE DOUBLE,
# MAGIC   NUMERATOR_UNIT_CONCEPT_ID LONG,
# MAGIC   DENOMINATOR_VALUE DOUBLE,
# MAGIC   DENOMINATOR_UNIT_CONCEPT_ID LONG,
# MAGIC   BOX_SIZE LONG,
# MAGIC   VALID_START_DATE DATE,
# MAGIC   VALID_END_DATE DATE,
# MAGIC   INVALID_REASON STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE RELATIONSHIP (
# MAGIC   RELATIONSHIP_ID STRING,
# MAGIC   RELATIONSHIP_NAME STRING,
# MAGIC   IS_HIERARCHICAL STRING,
# MAGIC   DEFINES_ANCESTRY STRING,
# MAGIC   REVERSE_RELATIONSHIP_ID STRING,
# MAGIC   RELATIONSHIP_CONCEPT_ID LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE SOURCE_TO_CONCEPT_MAP (
# MAGIC   SOURCE_CODE STRING,
# MAGIC   SOURCE_CONCEPT_ID LONG,
# MAGIC   SOURCE_VOCABULARY_ID STRING,
# MAGIC   SOURCE_CODE_DESCRIPTION STRING,
# MAGIC   TARGET_CONCEPT_ID LONG,
# MAGIC   TARGET_VOCABULARY_ID STRING,
# MAGIC   VALID_START_DATE DATE,
# MAGIC   VALID_END_DATE DATE,
# MAGIC   INVALID_REASON STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE VOCABULARY (
# MAGIC   VOCABULARY_ID STRING,
# MAGIC   VOCABULARY_NAME STRING,
# MAGIC   VOCABULARY_REFERENCE STRING,
# MAGIC   VOCABULARY_VERSION STRING,
# MAGIC   VOCABULARY_CONCEPT_ID LONG
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1, config
omop_version="vocab_542"
# project_name='omop-cdm-100K' # TO DO: see if this can be removed or if it is still necessary

vocab_s3_path = "s3://hls-eng-data-public/data/rwe/omop-vocabs-v542/" # DONE: updated this path to the new location of the OMOP V5.4 vocabularies

print(f"Using OMOP version {omop_version}")
print(f"Using vocabulary tables in {vocab_s3_path}")
# spark.sql(f"USE {omop_version}")

display(dbutils.fs.ls(vocab_s3_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading vocabularies as delta tables

# COMMAND ----------

# TO DO: run this code with the vocabularies for V5.4 

from pyspark.sql.functions import to_date
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

tablelist = ["CONCEPT","VOCABULARY","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","RELATIONSHIP","CONCEPT_SYNONYM","DOMAIN","CONCEPT_CLASS","DRUG_STRENGTH"]
for table in tablelist:
  df = spark.read.csv(f'{vocab_s3_path}/{table}.csv', inferSchema=True, header=True, dateFormat="yyyy-MM-dd")
  if table in ["CONCEPT","CONCEPT_RELATIONSHIP","DRUG_STRENGTH"] :
    if 'valid_start_date' in df.columns and 'valid_end_date' in df.columns:    df = df.withColumn('valid_start_date', to_date(df.valid_start_date,'yyyy-MM-dd')).withColumn('valid_end_date', to_date(df.valid_end_date,'yyyy-MM-dd'))

    
  df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(f"{catalog_name}.{schema_name}.{table}")

# COMMAND ----------

# DBTITLE 1,display tables and counts of records
tablecount = "SELECT '-' AS table, 0 as recs"
for table in tablelist:
  tablecount += " UNION SELECT '"+table+"', COUNT(1) FROM "+omop_version+"."+table
tablecount += " ORDER BY 2 DESC"

display(spark.sql(tablecount))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create vocab map tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### source_to_standard_vocab_map

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS source_to_standard_vocab_map;
# MAGIC
# MAGIC CREATE TABLE source_to_standard_vocab_map AS WITH CTE_VOCAB_MAP AS (
# MAGIC   SELECT
# MAGIC     c.concept_code AS SOURCE_CODE,
# MAGIC     c.concept_id AS SOURCE_CONCEPT_ID,
# MAGIC     c.concept_name AS SOURCE_CODE_DESCRIPTION,
# MAGIC     c.vocabulary_id AS SOURCE_VOCABULARY_ID,
# MAGIC     c.domain_id AS SOURCE_DOMAIN_ID,
# MAGIC     c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
# MAGIC     c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
# MAGIC     c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
# MAGIC     c.INVALID_REASON AS SOURCE_INVALID_REASON,
# MAGIC     c1.concept_id AS TARGET_CONCEPT_ID,
# MAGIC     c1.concept_name AS TARGET_CONCEPT_NAME,
# MAGIC     c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
# MAGIC     c1.domain_id AS TARGET_DOMAIN_ID,
# MAGIC     c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
# MAGIC     c1.INVALID_REASON AS TARGET_INVALID_REASON,
# MAGIC     c1.standard_concept AS TARGET_STANDARD_CONCEPT
# MAGIC   FROM
# MAGIC     CONCEPT C
# MAGIC     JOIN CONCEPT_RELATIONSHIP CR ON C.CONCEPT_ID = CR.CONCEPT_ID_1
# MAGIC     AND CR.invalid_reason IS NULL
# MAGIC     AND lower(cr.relationship_id) = 'maps to'
# MAGIC     JOIN CONCEPT C1 ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
# MAGIC     AND C1.INVALID_REASON IS NULL
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     source_code,
# MAGIC     SOURCE_CONCEPT_ID,
# MAGIC     SOURCE_CODE_DESCRIPTION,
# MAGIC     source_vocabulary_id,
# MAGIC     c1.domain_id AS SOURCE_DOMAIN_ID,
# MAGIC     c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
# MAGIC     c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
# MAGIC     c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
# MAGIC     stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
# MAGIC     target_concept_id,
# MAGIC     c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
# MAGIC     target_vocabulary_id,
# MAGIC     c2.domain_id AS TARGET_DOMAIN_ID,
# MAGIC     c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
# MAGIC     c2.INVALID_REASON AS TARGET_INVALID_REASON,
# MAGIC     c2.standard_concept AS TARGET_STANDARD_CONCEPT
# MAGIC   FROM
# MAGIC     source_to_concept_map stcm
# MAGIC     LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
# MAGIC     LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
# MAGIC   WHERE
# MAGIC     stcm.INVALID_REASON IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   CTE_VOCAB_MAP
# MAGIC   ;
# MAGIC SELECT * FROM source_to_standard_vocab_map LIMIT 100
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### source_to_source_vocab_map

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS source_to_source_vocab_map
# MAGIC ;
# MAGIC CREATE TABLE source_to_source_vocab_map AS WITH CTE_VOCAB_MAP AS (
# MAGIC   SELECT
# MAGIC     c.concept_code AS SOURCE_CODE,
# MAGIC     c.concept_id AS SOURCE_CONCEPT_ID,
# MAGIC     c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
# MAGIC     c.vocabulary_id AS SOURCE_VOCABULARY_ID,
# MAGIC     c.domain_id AS SOURCE_DOMAIN_ID,
# MAGIC     c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
# MAGIC     c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
# MAGIC     c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
# MAGIC     c.invalid_reason AS SOURCE_INVALID_REASON,
# MAGIC     c.concept_ID as TARGET_CONCEPT_ID,
# MAGIC     c.concept_name AS TARGET_CONCEPT_NAME,
# MAGIC     c.vocabulary_id AS TARGET_VOCABULARY_ID,
# MAGIC     c.domain_id AS TARGET_DOMAIN_ID,
# MAGIC     c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
# MAGIC     c.INVALID_REASON AS TARGET_INVALID_REASON,
# MAGIC     c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
# MAGIC   FROM
# MAGIC     CONCEPT c
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     source_code,
# MAGIC     SOURCE_CONCEPT_ID,
# MAGIC     SOURCE_CODE_DESCRIPTION,
# MAGIC     source_vocabulary_id,
# MAGIC     c1.domain_id AS SOURCE_DOMAIN_ID,
# MAGIC     c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
# MAGIC     c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
# MAGIC     c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
# MAGIC     stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
# MAGIC     target_concept_id,
# MAGIC     c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
# MAGIC     target_vocabulary_id,
# MAGIC     c2.domain_id AS TARGET_DOMAIN_ID,
# MAGIC     c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
# MAGIC     c2.INVALID_REASON AS TARGET_INVALID_REASON,
# MAGIC     c2.standard_concept AS TARGET_STANDARD_CONCEPT
# MAGIC   FROM
# MAGIC     source_to_concept_map stcm
# MAGIC     LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
# MAGIC     LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
# MAGIC   WHERE
# MAGIC     stcm.INVALID_REASON IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   CTE_VOCAB_MAP
# MAGIC ;
# MAGIC SELECT * FROM source_to_source_vocab_map LIMIT 100
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
# MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
# MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|

# COMMAND ----------


