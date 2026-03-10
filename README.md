## Data Warehouse and Analytics Project

# Welcome to the Data Warehouse and Analytics Project! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution — from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---
## 📐 Data Architecture
<img width="1599" height="878" alt="Data_Archture" src="https://github.com/user-attachments/assets/e923e818-d863-437d-aa2c-93cdac50fa29" />


The data architecture for this project follows the Medallion Architecture, structured across three layers


1. **🥉 Bronze**: BronzeStores raw data as-is from source systems. Data is ingested from CSV files into a SQL Server Database.
2. **🥈 Silver**: Applies data cleansing, standardization, and normalization to prepare data for analysis.
3. **🥇 Gold**: Houses business-ready data modeled into a star schema, optimized for reporting and analytics
---
## 📖 Project Overview

This project involves:

1. **Data Architecture**:Designing a modern data warehouse using Medallion Architecture (**Bronze**, **Silver**, and **Gold layers**)..
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:
- **🖥️ SQL Development**
- **🏗️ Data Architecture**
- **⚙️ Data Engineering**
- **🔄 ETL Pipeline Development**
- **🗂️ Data Modeling**
- **📊 Data Analytics**  

---
## 🚀 Project Requirements

### Building the Data Warehouse (*Data Engineering*)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

## Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

## 📊 BI: Analytics & Reporting (*Data Analysis*)
### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **👤 Customer Behavior**
- **📦 Product Performance**
- **📈 Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic and informed decision-making.
.

## 📂 Repository Structure
```
data-warehouse-project/
│
├── .github/                            # GitHub-specific configurations
│   ├── workflows/                      # CI/CD pipeline definitions (e.g., run tests on push)
│   └── PULL_REQUEST_TEMPLATE.md        # Standardized PR descriptions
│
├── datasets/                           # Raw source data (gitignored if sensitive)
│   ├── crm/                            # CRM source files
│   └── erp/                            # ERP source files
│
├── docs/                               # Project documentation
│   ├── architecture/                   # System and data architecture diagrams
│   │   ├── data_architecture.drawio
│   │   ├── data_flow.drawio
│   │   └── data_models.drawio          # Star schema diagrams
│   ├── etl/
│   │   └── etl.drawio                  # ETL techniques and methods
│   ├── data_catalog.md                 # Field descriptions and metadata per layer
│   ├── naming-conventions.md           # Naming standards for tables, columns, files
│   └── runbook.md                      # How to deploy, run, and troubleshoot the pipeline
│
├── scripts/                            # All SQL scripts
│   ├── init/                           # One-time setup: create databases, schemas, roles
│   ├── bronze/                         # Raw ingestion — minimal transformation
│   │   ├── crm/
│   │   └── erp/
│   ├── silver/                         # Cleaned, standardized, deduplicated data
│   │   ├── crm/
│   │   └── erp/
│   └── gold/                           # Analytical models — facts, dims, aggregates
│       ├── dimensions/
│       └── facts/
│
├── tests/                              # Data quality and validation
│   ├── bronze/                         # Null checks, row count validations
│   ├── silver/                         # Dedup checks, type conformance
│   └── gold/                           # Business logic and metric validations
│
├── logs/                               # Pipeline run logs (gitignored)
│
├── .gitignore
├── LICENSE
├── README.md                           # Project overview, setup guide, layer descriptions
├── CHANGELOG.md                        # Version history and notable changes
└── requirements.txt                    # Dependencies (e.g., dbt, sqlfluff, great_expectations)
```

## 🌟 About Me
Hi, I'm Stewart Ayim, an aspiring Data Engineer with a passion for data workflows, pipeline architecture, and analytics engineering. I'm committed to continuous learning and building hands-on projects that sharpen my skills across the full data stack.

## Acknowledgements🦾
A huge thank you to Baraa Khatib Salkini for mentoring me through this project. His guidance was invaluable — I gained a tremendous amount of knowledge and hands-on experience that I'll carry forward in my career.

Connect with me on LinkedIn @ https://www.linkedin.com/in/stewartayim/
