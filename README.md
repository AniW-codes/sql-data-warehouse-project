# Data Warehouse and Analytics Project

This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.


## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:


![Data Architecture](https://github.com/AniW-codes/sql-data-warehouse-project/blob/main/High_Level_Architecture.png)


1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.



## 🏗️ Database Name: Datawarehouse1

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

This repository showcases builiding Data Warehouse from scratch, looking to showcase expertise in:
- SQL Development
- Data Architect
- Data Engineering  
- ETL Pipeline Developer  
- Data Modeling  
- Data Analytics  

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                                         # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                                             # Project documentation and architecture details
│   ├── DB_Warehouse.sql                              # Script for data base creation
│   ├── Data Flow Diagram.drawio                      # Draw.io file for the data flow diagram
│   ├── Data Mart.drawio                              # Final level project architecture of 2 dim and 1 fact table joining
│   ├── High Level Architecture.drawio                # Data flow architecture leading to formation of the data warehouse
│   ├── Integration_model.drawio                      # Integration level architechute of columns that can be used for joins
│   ├── Silver_Cust_Info_Ingestion.sql                # Ingestion of customer info from bronze layer into silver layer (cleaned, transformed)
│   ├── Silver_Prd_Info_Ingestion.sql                 # Ingestion of products info from bronze layer into silver layer (cleaned, transformed)
│   ├── Silver_Sales_Details_Ingestion.sql            # Ingestion of sales info from bronze layer into silver layer (cleaned, transformed)
│   ├── Silver_erp_cust_az12_Ingestion.sql            # Ingestion of meta data of customer (birthdate) from bronze layer into silver layer (cleaned, transformed)
│   ├── Silver_erp_loc_a101_Ingestion.sql             # Ingestion of meta data of customer (country) from bronze layer into silver layer (cleaned, transformed)
│   ├── Silver_erp_px_cat_g1v2_Ingestion.sql          # Ingestion of meta data of products (category, subcategory, ) from bronze layer into silver layer(cleaned, transformed)
│   ├── stored_procedure_silver_layer.sql             # Stored Procedure to automate ingestion of data from multiple raw sources (bronze layer) into **cleaned** silver layer
│   ├── stored_procedure_bronze_layer.sql             # Stored Procedure to automate ingestion of data from csv files into as it is **raw** form in bronze layer 
│   ├── data_catalog.md                               # Overview level information of kinds of data-types, column headers and their relevant description in **GOLD layer**.
│   ├── bronze_layer.sql                              # Creating tables and headers for future data transformations,stored procedures - builiding blocks of bronze layer.
│   ├── silver_layer.sql                              # Creating tables and headers for future data aggregations, views to gold layer.
│
├── scripts/                                          # SQL scripts for ETL and transformations
│   ├── bronze/                                       # Scripts for extracting and loading raw data
│   ├── silver/                                       # Scripts for cleaning and transforming data
│   ├── gold/                                         # Scripts for creating analytical models
│
├── tests/                                            # Test scripts and quality files
│   ├── bronze/                                       # Test scripts for base level data
│   ├── silver/                                       # Test scripts to validate quality of clean data
│   ├── gold/                                         # Test scripts to validate final ready-to-use data
│
├── README.md                                         # Project overview and instructions
├── LICENSE                                           # License information for the repository
├── .gitignore                                        # Files and directories to be ignored by Git
```
---


## Licence

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me
Feel free to connect with me on the following platforms:

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/aniruddhawarang/)
