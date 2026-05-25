# Brazilian Educational Census ETL Pipeline with Apache Spark

## 📌 Project Overview
This project processes the Brazilian National Basic Education Census data from the Brazilian Government Open Data Portal using an ETL (Extract, Transform, Load) pipeline built with Apache Spark.

The raw datasets contain educational information collected between **2010–2020**, including:
- School infrastructure
- Teacher statistics
- Student enrollments
- Geographic information
- Ethnic and economic indicators

The pipeline transforms large and unstructured CSV files into a structured **Star Schema** stored in PostgreSQL for Business Intelligence (BI) reporting and dashboard analysis.

---

# 🧠 Business Understanding

## 1.1 Project Overview
The objective of this project is to build a scalable ETL pipeline capable of processing large-scale educational census datasets using Apache Spark.

The final architecture converts raw CSV files into:
- Optimized Parquet files
- Structured Star Schema tables
- PostgreSQL data warehouse tables

This enables:
- Faster analytical queries
- Dashboard creation
- Business Intelligence reporting
- Historical trend analysis

---

## 1.2 Business Problem

The raw educational census data presents several challenges:

- Large file size (~2.2GB total)
- Approximately 370 columns per dataset
- Traditional tools such as Pandas are inefficient for processing
- Difficult to perform analytics directly from raw CSV files

---

## 1.3 Business Objectives

The project aims to:

- Build a scalable ETL pipeline using Apache Spark
- Convert raw CSV files into compressed Parquet format
- Design a Star Schema data warehouse
- Store processed data in PostgreSQL
- Support BI dashboards and analytical reporting

---

# 📊 Data Understanding

The dataset comes from the Brazilian National Basic Education Census (**Censo Escolar**) publicly available through the Brazilian Government Open Data Portal.

## 2.1 Dataset Characteristics

| Attribute | Description |
|---|---|
| Data Type | CSV |
| Time Period | 2010–2020 |
| Number of Columns | ~370 |
| Dataset Size | ~2.2GB |
| Estimated Records | ~3 million rows |

---

## 2.2 Relational Data Model

The raw data is transformed into a **Star Schema** to improve analytical performance.

---

# ⭐ Star Schema Design

## Fact Table: `fact_censo_escolar`

This table stores educational metrics and connects to multiple dimensions.

| Column Name | Description |
|---|---|
| QT_DOC_BAS | Total teachers |
| QT_DOC_INF | Child education teachers |
| QT_DOC_FUND | Elementary education teachers |
| QT_DOC_MED | High school teachers |
| QT_MAT_BAS | Total enrollments |
| QT_MAT_INF | Child education enrollments |
| QT_MAT_FUND | Elementary education enrollments |
| QT_MAT_MED | High school enrollments |
| QT_MAT_BAS_ND | Race not declared enrollments |
| QT_MAT_BAS_BRANCA | White race enrollments |
| QT_MAT_BAS_PRETA | Black race enrollments |
| QT_MAT_BAS_PARDA | Brown race enrollments |
| QT_MAT_BAS_AMARELA | Yellow race enrollments |
| QT_MAT_BAS_INDIGENA | Indigenous enrollments |
| NU_ANO_CENSO | Census year |

---

# 📂 Dimension Tables

## `dim_local`

| Column | Description |
|---|---|
| NO_UF | State name |
| SG_UF | State abbreviation |
| CO_UF | State code |
| NO_MUNICIPIO | City name |
| CO_MUNICIPIO | City code |

---

## Other Dimensions

| Dimension Table | Description |
|---|---|
| dim_tp_localizacao | Urban / Rural |
| dim_tp_dependencia | School administration type |
| dim_in_agua_potavel | Access to drinkable water |
| dim_in_energia_inexistente | No electricity |
| dim_in_esgoto_inexistente | No sewage |
| dim_in_equip_nenhum | No electronic equipment |
| dim_in_internet | Internet availability |
| dim_in_computador | Computer availability |
| dim_in_refeitorio | School canteen |
| dim_in_biblioteca | Library availability |
| dim_in_banheiro | Restroom availability |

---

# ⚙️ Data Preparation

The ETL pipeline performs the following processes:

## Environment Configuration
- Configure Hadoop path variables
- Customize Spark memory allocation
- Configure PostgreSQL JDBC driver

## Data Extraction
- Use Python `glob` library to locate yearly CSV files
- Read multiple CSV files into a unified Spark DataFrame
- Use semicolon (`;`) delimiter and proper text encoding

## Data Transformation
- Avoid slow schema auto-detection
- Standardize data types
- Clean and restructure datasets

## Parquet Conversion
The raw CSV files are converted into **Parquet format** to:
- Improve query performance
- Reduce storage usage
- Enable columnar compression
- Optimize Spark processing

---

# 🏗️ Data Modeling

## 4.1 Star Schema Architecture

The original unnormalized datasets are transformed into a multidimensional Star Schema optimized for OLAP analytics.

The architecture contains:
- 1 Fact table
- 12 Dimension tables

This structure improves:
- Query performance
- Dashboard responsiveness
- Data aggregation efficiency

---

## 4.2 Dimension Table Configuration

Dimension tables are dynamically created by:

- Selecting distinct categorical values
- Removing duplicates using `.distinct()`
- Creating surrogate primary keys
- Writing dimensions into PostgreSQL

---

## 4.3 Fact Table Architecture

The fact table contains:
- Enrollment metrics
- Teacher statistics
- Foreign keys linked to dimensions

Apache Spark performs multiple left joins to map:
- Raw CSV data
- Dimension surrogate keys
- Numerical metrics

The final dataset is loaded into PostgreSQL using JDBC connections.

---

# 📈 Evaluation

## 5.1 Pipeline Performance Metrics

| Evaluation Metric | Captured Value | Meaning |
|---|---|---|
| Total Ingestion Volume | 2,792,984 rows | Total educational records processed |
| Data Quality Retention | 100% | No rows dropped |
| Total Pipeline Latency | 418.38 seconds | End-to-end ETL execution time |
| Raw Storage Volume | 2.54 GB | Original CSV storage |
| Optimized Parquet Size | 322.25 MB | Compressed Parquet size |
| Storage Compression | 87.3% reduction | Storage optimization achieved |
| Throughput Efficiency | ~6,677 rows/sec | Spark processing throughput |

---

## 5.2 Business Impact

The pipeline successfully achieved the project objectives:

✅ Scalable Spark-based ETL architecture  
✅ Efficient processing of ~2.8 million rows  
✅ Significant storage reduction through Parquet conversion  
✅ Structured Star Schema for BI reporting  
✅ High data integrity with zero row loss  

The transformed data supports advanced educational analytics such as:
- Internet access vs teacher distribution
- Enrollment trends by state
- Infrastructure availability analysis

---

# ⚠️ System Limitations

## 1. Parquet Write Performance
Parquet conversion on Windows environments showed slower write performance during local execution.

## 2. JDBC Bottleneck
PostgreSQL JDBC writes became the slowest stage due to transactional relational database constraints.

## 3. Batch Processing Constraints
The pipeline currently operates in batch mode, requiring full reruns whenever new data is added.

---

# 🚀 Future Improvements

Several enhancements are proposed for enterprise scalability:

## Cloud Migration
Move to cloud platforms such as:
- Azure Databricks
- Azure Synapse Analytics

Benefits:
- Managed Spark clusters
- Better scalability
- Reduced Windows compatibility issues

---

## Medallion Architecture

Implement:
- Bronze Layer → Raw CSV
- Silver Layer → Cleaned Parquet
- Gold Layer → Final Star Schema

This improves:
- Data lineage
- Data auditing
- Rollback capabilities

---

## Cloud Data Warehouse
Replace PostgreSQL with:
- Snowflake
- Azure Synapse SQL Pool

Benefits:
- Parallel data loading
- Faster query execution
- Elimination of JDBC bottlenecks

---

## Automated Data Quality Validation
Integrate tools such as:
- Great Expectations

Benefits:
- Automatic anomaly detection
- Schema validation
- Data quarantine mechanisms

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Python | ETL scripting |
| Apache Spark | Distributed data processing |
| PostgreSQL | Data warehouse |
| Docker | Containerization |
| Metabase | BI dashboards |
| Parquet | Optimized columnar storage |

---

# 💭 Reflection
This project was challenging because it required integrating multiple technologies including Python, Apache Spark, Docker, PostgreSQL, and Metabase.

One of the main challenges was environment setup, particularly adapting Linux-based tutorials to a Windows environment using WSL. Additional configuration was needed for:
- PostgreSQL JDBC drivers
- Apache Spark setup
- Hadoop path configuration

Team collaboration played an important role throughout the project. Group discussions, troubleshooting sessions, and collaborative debugging helped resolve technical issues effectively.

Overall, this project strengthened:
- Data engineering knowledge
- Problem-solving skills
- Team collaboration
- Real-world ETL pipeline development experience

---

# 📌 Conclusion

This project demonstrates how Apache Spark can efficiently process large-scale educational datasets and transform them into a scalable analytical architecture.

The ETL pipeline successfully:
- Processed millions of records
- Reduced storage size significantly
- Built a BI-ready Star Schema
- Enabled high-performance analytical reporting

The solution provides a strong foundation for future cloud-based big data and business intelligence implementations.
