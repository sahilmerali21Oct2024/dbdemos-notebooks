# Quick Start Guide - Team Validation

## 🚀 Getting Started in 5 Minutes

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Review the Conversion
```bash
# Read the main documentation
cat README_PYSPARK.md

# Review technical details  
cat CONVERSION_SUMMARY.md

# Check team validation guide
cat TEAM_VALIDATION_GUIDE.md
```

### 3. Test the Implementation
```bash
# Run the demonstration
python demo_synthea_tables.py

# Create configuration file
python run_synthea_etl.py --create-config

# Test with sample data (after configuring paths)
python run_synthea_etl.py --config synthea_etl_config.yaml --validate-only
```

### 4. Compare with Original R Code
- Check `original_r_code/` directory for R reference files
- Compare logic in `synthea_omop_etl.py` vs `codeToRun.R`
- Review table creation in `synthea_table_creator.py` vs `CreateSyntheaTables.r`

## 📋 Validation Checklist

### Code Review
- [ ] Review `synthea_omop_etl.py` - Main ETL logic
- [ ] Review `synthea_table_creator.py` - Table creation
- [ ] Review `synthea_transformations.py` - OMOP transformations
- [ ] Review `omop_cdm_schemas.py` - Schema definitions

### Functional Testing
- [ ] Test table creation with different Synthea versions
- [ ] Test data loading and transformation logic
- [ ] Validate OMOP CDM table structure and relationships
- [ ] Compare outputs with R version (if available)

### Performance Testing
- [ ] Test with large datasets
- [ ] Measure memory usage and execution time
- [ ] Validate scalability on cluster

## 🆘 Need Help?

- **Usage Questions**: See `README_PYSPARK.md`
- **Technical Details**: See `CONVERSION_SUMMARY.md` 
- **Validation Process**: See `TEAM_VALIDATION_GUIDE.md`
- **Examples**: Run `python demo_synthea_tables.py`

## ✅ Success Criteria

The conversion is successful if:
1. All PySpark functions work equivalently to R functions
2. Table schemas match OMOP CDM specification exactly  
3. Data transformations produce identical results to R version
4. Performance is equal or better than R implementation
5. Code is maintainable and follows Python best practices
