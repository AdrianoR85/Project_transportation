# End-to-End Transportation Data Engineering Pipeline
This project demonstrates an enterprise-grade data engineering workflow built using Databricks and LakeFlow Spark Declarative Pipelines (SDP). The primary goal was to modernize a legacy procedural pipeline for a transportation company (Good Cabs) to improve data delivery speeds and regional analytical capabilities.

## Key Features & Technologies
Declarative Pipelines (SDP): 
  - Transitioned from manual,
  - imperative orchestration to a declarative approach,
  - allowing the system to handle execution plans,
  - dependencies,
  - incremental processing automatically.

## Data Source
The raw data used in this project is stored in Amazon S3, serving as the ingestion layer for the pipeline.
Databricks Auto Loader is used to incrementally ingest data into the Bronze layer.

- Storage: Amazon S3
- Format: CSV
- Ingestion: Auto Loader (incremental)


## Medallion Architecture: 
Implemented a robust data architecture using:
- Bronze Layer: Raw data ingestion from Amazon S3 using Auto Loader for incremental processing.
- Silver Layer: Data cleaning, transformation, and business logic application.
- Gold Layer: BI-ready aggregated data tailored for regional analytics.

## Data Governance & Access: 
- Utilized Unity Catalog for centralized access management, enabling Role-Based Access Control (RBAC) for regional managers 
- Unified Analytics: Leveraged built-in AI features (Genie) for natural language data querying and rapid insights.

## 🛠️ Technology Stack

- **Language**: Python
- **Notebooks**: Jupyter Notebooks
- **Data Platform**: Databricks
- **Architecture**: Medallion Architecture (Delta Lake)
- **Amazon S3**: Data Source

## 📁 Project Structure
```
Project_transportation / 
  |-- transportation_pipeline/
  |-- README.md
  |-- Project documentation 
  |-- project_setup.ipynb # Initial setup and configuration 
  |__ .gitignore # Git ignore rules
```

## 🚀 Step-by_step

### 1. Create a GitHub Repository
First, create a repository on github,then go to Databricks.

### 2. Open Databricks Workspace:
The Workspace is where you organize, write, and run your notebooks.
  - Go to Create → Git Folder
  - Fill in the repository details
  - Open the created folder
  - Create a new Notebook

 #### 🧩 Create a Widget (User Input)
  ```python
      dbutils.widgets.text("catalog_name", "transportation", "Catalog Name")
      catalog_name = dbutils.widgets.get("catalog_name") 
      catalog_name
  ```
  ##### What this does:
  `dbutils.widgets.text(name, defaultValue, label)` creates an input box in the UI.
  - name → variable used in code
  - defaultValue → default value shown
  - label → display name in the UI 

  #### 🏗️ Create Catalog
  ```spark 
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
  ```
  #### 🗂️ Create Schemas
  ```sql
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze;")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver;")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold;")
  ```
  ##### What this does:
  - `spark.sql()` is a method used in Apache Spark to run **SQL queries directly on Spark.**
   - It lets you write SQL (like `SELECT`, `CREATE`, `INSERT`) and execute it on your data using Spark.
   - **Catalog** is like a main folder that contains databases (schemas), tables, and other data assets.
   - In this case, it create a **catalog** with the value set at **catalog_name**, and create **Schemas** inside the catalog_name.


## 🚀 Getting Started

### Prerequisites

- Databricks workspace access
- Apache Spark cluster running
- Python 3.x environment
- AWS Account (free)
