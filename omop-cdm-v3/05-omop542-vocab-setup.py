# Databricks notebook source
# MAGIC %md
# MAGIC # OMOP Vocabulary Setup
# MAGIC Construct vocabulary tables, based on tables downloaded from [Athena](https://athena.ohdsi.org/search-terms/start) website and available here on `s3://hls-eng-data-public/data/rwe/omop-vocabs-v542/`
# MAGIC If you like to download a different dataset, downoad the vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) and
# MAGIC use [databricks dbfs api](https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfs-api) utilities to upload downloaded vocabularies to `dbfs` under your `vocab_path`.
# MAGIC
# MAGIC <img align="right" width="700"  src="https://drive.google.com/uc?export=view&id=16TU2l7XHjQLugmS_McXegBXKMglD--Fr">

# COMMAND ----------

dbutils.widgets.text(name = "catalog_name", defaultValue="hls_omop", label="Catalog Name")
dbutils.widgets.text(name = "schema_name", defaultValue="vocab_542", label="Schema Name")
# # dbutils.widgets.text(name = "volume_name", defaultValue="omop_vocab", label="Volume Name")

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

# %sql
# CREATE
# OR REPLACE TABLE SOURCE_TO_CONCEPT_MAP (
#   SOURCE_CODE STRING,
#   SOURCE_CONCEPT_ID LONG,
#   SOURCE_VOCABULARY_ID STRING,
#   SOURCE_CODE_DESCRIPTION STRING,
#   TARGET_CONCEPT_ID LONG,
#   TARGET_VOCABULARY_ID STRING,
#   VALID_START_DATE DATE,
#   VALID_END_DATE DATE,
#   INVALID_REASON STRING
# ) USING DELTA;

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

from pyspark.sql.functions import to_date, col

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

tablelist = ["CONCEPT","VOCABULARY","CONCEPT_ANCESTOR","CONCEPT_RELATIONSHIP","RELATIONSHIP","CONCEPT_SYNONYM","DOMAIN","CONCEPT_CLASS","DRUG_STRENGTH"]
for table in tablelist:
    df = spark.read.csv(
        f'{vocab_s3_path}/{table}.csv',
        inferSchema=True,
        header=True,
        dateFormat="yyyymmdd",
        sep="\t"
    )
    
    if table in ["CONCEPT", "CONCEPT_RELATIONSHIP", "DRUG_STRENGTH"]:
        if 'valid_start_date' in df.columns and 'valid_end_date' in df.columns:
            df = df.withColumn('valid_start_date', to_date(df.valid_start_date, 'yyyymmdd')).withColumn('valid_end_date', to_date(df.valid_end_date, 'yyyymmdd'))
    
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f"{catalog_name}.{schema_name}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create vocab map tables

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
# MAGIC     source_to_concept_map stcm -- TO DO: Find out how the source_to_concept_map table is populated with data
# MAGIC     LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
# MAGIC     LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
# MAGIC   WHERE
# MAGIC     stcm.INVALID_REASON IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   CTE_VOCAB_MAP;

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
# MAGIC     source_to_concept_map stcm -- TO DO: Find out how the source_to_concept_map table is populated with data
# MAGIC     LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
# MAGIC     LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
# MAGIC   WHERE
# MAGIC     stcm.INVALID_REASON IS NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   CTE_VOCAB_MAP;
# MAGIC
