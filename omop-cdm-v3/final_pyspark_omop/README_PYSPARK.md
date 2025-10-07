# Synthea OMOP CDM ETL - PySpark Implementation

This project provides a **PySpark implementation** of the ETL process to convert [Synthea](https://synthetichealth.github.io/synthea/) synthetic patient data to the [OMOP Common Data Model (CDM)](https://github.com/OHDSI/CommonDataModel). 

This is a **Python/PySpark conversion** of the original R-based [ETLSyntheaBuilder](https://github.com/OHDSI/ETLSyntheaBuilder) package, maintaining the same OMOP mapping logic and SQL transformations while leveraging PySpark for scalable data processing.

## 🔄 Conversion from R to PySpark

This implementation converts:
- **R DatabaseConnector** → **PySpark DataFrame operations**
- **R data.table operations** → **PySpark DataFrame transformations** 
- **R SQL scripts** → **PySpark SQL and DataFrame API**
- **R functions** → **Python classes and methods**
- **R package structure** → **Modular Python architecture**

All original OMOP mapping logic, vocabulary mappings, and concept transformations are preserved.

## ✨ Features

- **Scalable Processing**: Leverages PySpark for distributed processing of large datasets
- **Complete OMOP Coverage**: Supports all OMOP CDM tables and relationships
- **Multiple CDM Versions**: Compatible with CDM v5.3 and v5.4
- **Synthea Versions**: Supports Synthea versions 2.7.0, 3.0.0, 3.1.0, 3.2.0, and 3.3.0
- **Flexible Configuration**: YAML-based configuration for easy deployment
- **Data Validation**: Built-in data quality checks and validation queries
- **Production Ready**: Comprehensive logging, error handling, and monitoring

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd ETL-Synthea

# Install Python dependencies
pip install -r requirements.txt

# Ensure Java 8 or 11 is installed (required for PySpark)
java -version
```

### 2. Generate Configuration File

```bash
# Create a default configuration template
python run_synthea_etl.py --create-config
```

This creates `synthea_etl_config.yaml` which you need to customize:

```yaml
spark_config:
  app_name: SyntheaOMOPETL
  warehouse_dir: /tmp/spark-warehouse
  database_name: cdm_synthea_v540

etl_config:
  cdm_version: '5.4'
  synthea_version: '2.7.0' 
  synthea_schema: synthea_v270
  synthea_file_loc: /path/to/synthea/output/csv     # ← Update this path
  vocab_file_loc: /path/to/vocab/csv                # ← Update this path

cdm_source_config:
  cdm_source_name: Synthea synthetic health database
  cdm_source_abbreviation: Synthea
  cdm_holder: OHDSI
  cdm_source_description: SyntheaTM is a Synthetic Patient Population Simulator

performance_config:
  create_extra_indices: true
  cache_tables: true
```

### 3. Prepare Data

**Synthea Data:**
```bash
# Generate Synthea data (follow Synthea documentation)
git clone https://github.com/synthetichealth/synthea.git
cd synthea
./gradlew build
./run_synthea -p 1000  # Generate data for 1000 patients
# CSV files will be in output/csv/
```

**OMOP Vocabulary:**
- Download from [OHDSI Athena](https://athena.ohdsi.org/)
- Extract to a directory and note the path

### 4. Run ETL Process

```bash
# Run the complete ETL pipeline
python run_synthea_etl.py --config synthea_etl_config.yaml
```

### 5. Validate Results

```bash
# Run validation queries
python run_synthea_etl.py --config synthea_etl_config.yaml --validate-only
```

## 📋 Equivalent R Workflow

This PySpark implementation replicates the exact workflow from the original R package:

| R Function | PySpark Equivalent | Description |
|------------|-------------------|-------------|
| `createConnectionDetails()` | `SparkSession.builder` | Database connection setup |
| `CreateCDMTables()` | `create_cdm_tables()` | Create OMOP CDM table structures |
| `CreateSyntheaTables()` | `create_synthea_tables()` | Create Synthea staging tables |
| `LoadSyntheaTables()` | `load_synthea_tables()` | Load CSV data into staging tables |
| `LoadVocabFromCsv()` | `load_vocab_from_csv()` | Load OMOP vocabulary tables |
| `CreateMapAndRollupTables()` | `create_map_and_rollup_tables()` | Create mapping and visit rollup tables |
| `CreateExtraIndices()` | `create_extra_indices()` | Performance optimization |
| `LoadEventTables()` | `load_event_tables()` | Transform data to OMOP CDM format |

### Original R Code (codeToRun.R):
```r
library("ETLSyntheaBuilder")
library("DatabaseConnector")

connectionDetails <- createConnectionDetails(...)
CreateCDMTables(connectionDetails, cdmDatabaseSchema, cdmVersion)
CreateSyntheaTables(connectionDetails, syntheaSchema, syntheaVersion)
LoadSyntheaTables(connectionDetails, syntheaSchema, syntheaFileLoc)
LoadVocabFromCsv(connectionDetails, cdmDatabaseSchema, vocabFileLoc)
CreateMapAndRollupTables(connectionDetails, cdmDatabaseSchema, syntheaSchema, cdmVersion, syntheaVersion)
CreateExtraIndices(connectionDetails, cdmDatabaseSchema, syntheaSchema, syntheaVersion)
LoadEventTables(connectionDetails, cdmDatabaseSchema, syntheaSchema, cdmVersion, syntheaVersion)
```

### PySpark Equivalent:
```python
from synthea_omop_etl import SyntheaOMOPETL

etl = SyntheaOMOPETL(database_name="cdm_synthea_v540")
etl.run_full_etl(
    cdm_version="5.4",
    synthea_file_loc="/path/to/synthea/csv",
    vocab_file_loc="/path/to/vocab/csv", 
    synthea_version="2.7.0"
)
```

## 🏗️ Architecture

### Project Structure
```
ETL-Synthea/
├── synthea_omop_etl.py          # Main ETL orchestration class
├── synthea_transformations.py   # All SQL transformation logic
├── omop_cdm_schemas.py          # OMOP CDM and Synthea table schemas
├── run_synthea_etl.py           # Command-line runner script
├── requirements.txt             # Python dependencies
├── README_PYSPARK.md           # This documentation
└── synthea_etl_config.yaml     # Configuration template
```

### Key Classes

**`SyntheaOMOPETL`**: Main orchestration class that manages the complete ETL workflow
- Spark session management
- Table creation and data loading
- ETL step coordination
- Error handling and logging

**`SyntheaTransformations`**: Contains all SQL transformation logic converted from R
- Vocabulary mapping creation
- Visit rollup logic
- All OMOP table transformations
- Concept ID resolution

## 📊 Supported OMOP Tables

The implementation creates and populates all core OMOP CDM tables:

### Clinical Data Tables
- **PERSON** - Patient demographics
- **OBSERVATION_PERIOD** - Patient observation periods
- **VISIT_OCCURRENCE** - Healthcare encounters
- **VISIT_DETAIL** - Visit details and sub-visits
- **CONDITION_OCCURRENCE** - Diagnosed conditions
- **DRUG_EXPOSURE** - Medication exposures and immunizations
- **PROCEDURE_OCCURRENCE** - Medical procedures
- **DEVICE_EXPOSURE** - Medical devices
- **MEASUREMENT** - Laboratory values and vital signs
- **OBSERVATION** - Clinical observations
- **DEATH** - Patient death information

### Health System Tables
- **LOCATION** - Geographic locations
- **CARE_SITE** - Healthcare facilities
- **PROVIDER** - Healthcare providers

### Health Economics Tables
- **PAYER_PLAN_PERIOD** - Insurance coverage periods
- **COST** - Healthcare costs and claims

### Era Tables
- **CONDITION_ERA** - Condition eras
- **DRUG_ERA** - Drug eras

### Metadata Tables
- **CDM_SOURCE** - Data source information
- **METADATA** - ETL metadata

## 🔧 Advanced Configuration

### Spark Configuration

```yaml
spark_config:
  app_name: SyntheaOMOPETL
  warehouse_dir: /data/spark-warehouse
  database_name: cdm_synthea_v540
  # Additional Spark configs can be added here
```

### Performance Tuning

```yaml
performance_config:
  create_extra_indices: true      # Create performance indices
  cache_tables: true              # Cache frequently accessed tables
  partition_strategy: "patient_id" # Partitioning strategy
  num_partitions: 100             # Number of partitions
```

### Custom Transformations

You can extend the transformations by subclassing `SyntheaTransformations`:

```python
from synthea_transformations import SyntheaTransformations

class CustomTransformations(SyntheaTransformations):
    def custom_transform_person(self):
        # Add your custom person transformation logic
        pass
```

## 🧪 Testing and Validation

### Built-in Validation

The ETL includes comprehensive validation:

```bash
# Run with validation enabled
python run_synthea_etl.py --config config.yaml --validate-only
```

Validation includes:
- Record counts for all tables
- Data type validation
- Referential integrity checks
- Concept ID validation
- Date range validation

### Custom Validation Queries

Add custom validation queries to your config:

```yaml
validation_queries:
  - name: "Person age distribution"
    query: "SELECT YEAR(CURRENT_DATE) - year_of_birth as age, COUNT(*) FROM person GROUP BY age"
  - name: "Visit types"
    query: "SELECT visit_concept_id, COUNT(*) FROM visit_occurrence GROUP BY visit_concept_id"
```

## 🔍 Monitoring and Logging

### Comprehensive Logging

- All ETL steps are logged with timestamps
- Progress tracking for large table transformations
- Error details with stack traces
- Performance metrics and timing

### Log Files

```bash
# ETL execution logs
tail -f synthea_etl.log

# Spark application logs (if running on cluster)
yarn logs -applicationId application_xxx
```

## 🚀 Production Deployment

### Spark Cluster Deployment

```python
# Configure for cluster deployment
etl = SyntheaOMOPETL(
    app_name="SyntheaOMOPETL_Production",
    # Add cluster-specific configurations
)
```

### Docker Deployment

Create a Dockerfile:

```dockerfile
FROM apache/spark-py:v3.3.2

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY *.py /app/
WORKDIR /app

CMD ["python", "run_synthea_etl.py", "--config", "config.yaml"]
```

### Environment Variables

```bash
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip
export JAVA_HOME=/path/to/java
```

## 🤝 Comparison with R Implementation

### Advantages of PySpark Implementation

1. **Scalability**: Handle much larger datasets through distributed processing
2. **Performance**: Optimized execution engine with adaptive query execution
3. **Memory Management**: Better handling of large datasets through lazy evaluation
4. **Integration**: Native integration with big data ecosystems (Hadoop, Delta Lake, etc.)
5. **Deployment**: Container-friendly and cloud-native deployment options

### Functional Equivalence

- ✅ **Same OMOP Mapping Logic**: All vocabulary mappings and concept transformations preserved
- ✅ **Same SQL Logic**: All transformation SQL converted to PySpark SQL
- ✅ **Same Data Quality**: Identical data validation and quality checks
- ✅ **Same Output**: Produces identical OMOP CDM tables and structure

## 📈 Performance Considerations

### Memory Management

```python
# Configure Spark for large datasets
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### Partitioning Strategy

```python
# Partition large tables by patient_id for better performance
df.repartition("patient_id").write.saveAsTable("table_name")
```

### Caching Strategy

```python
# Cache intermediate results
df.cache()
df.count()  # Trigger caching
```

## 🐛 Troubleshooting

### Common Issues

**Java Version Compatibility**:
```bash
# Use Java 8 or 11
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

**Memory Issues**:
```bash
# Increase Spark driver memory
export SPARK_DRIVER_MEMORY=4g
```

**File Path Issues**:
```yaml
# Use absolute paths in configuration
synthea_file_loc: /full/path/to/synthea/csv
vocab_file_loc: /full/path/to/vocab/csv
```

### Debug Mode

```python
# Enable debug logging
import logging
logging.getLogger('synthea_omop_etl').setLevel(logging.DEBUG)
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Submit a pull request

## 📄 License

Same license as the original R ETLSyntheaBuilder package.

## 🙏 Acknowledgments

- Original R ETLSyntheaBuilder package developers
- OHDSI collaborative community
- Synthea development team
- Apache Spark community

## 📞 Support

For questions about this PySpark implementation:
- Create an issue in the repository
- Reference the original R package documentation
- Consult OMOP CDM documentation

---

This PySpark implementation maintains 100% functional equivalence with the original R package while providing the scalability and performance benefits of Apache Spark.
