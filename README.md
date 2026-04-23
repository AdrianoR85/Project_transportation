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

### 1️⃣ Create a GitHub Repository
First, create a repository on github,then go to Databricks.

--- 

### 2️⃣ Open Databricks Workspace:
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
  ##### What this does
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
  ##### What this does
  - ```spark.sql()``` is a method used in Apache Spark to run **SQL queries directly on Spark.**
  - It lets you write SQL (like `SELECT`, `CREATE`, `INSERT`) and execute it on your data using Spark.
  - **Catalog** is like a main folder that contains databases (schemas), tables, and other data assets.
  - In this case, it create a **catalog** with the value set at **catalog_name**, and create **Schemas** inside the catalog_name.

--- 

### 3️⃣ S3 Bucket Setup
An S3 bucket is a container used to store files (objects) in the cloud.

#### 🏗️ How it's structured
Inside the Amazon S3:

```
Bucket
|___Folder
    |____Object(file or folder)
         |_____ Data + Metabata.
```

#### **🚢 How to create a Bucket:**
1. Gi to **AWS Console**
2. Open S3
3. Click **"Create bucket"**
4. Fill in:
   - **Bucket name**: must be globally unique
   - **Region**: Choose close to your data processing
5. Keep **Block all public access** enabled (recommended)
6. Click **Create bucket**

#### **📁 How to create a folder:**
1. Open your bucket in S3
2. Click **Creata folder**
3. Enter a name in **Folder name**
4. Click **Create**

#### **📄 How to insert files:**
1. Open your folder in bucket
2. Drag and Drop your files or click **Add files** to insert the files.
3. Click **Upload**

#### **🔗 How to connect to Databricks:
1. Go to Catalog in Databricks
2. Click **External locations** in ⚙️
3. Click **Create external location**
4. Click **AWS Quickstart (Recommendend)** and after **Next**
5. Enter the **Bucket Name** (URL: s3://your_bucket_name)
6. Click **Generate new token** and copy it.
7. Click **Launch in Quickstart**
8. On the window that opened, past the token in **Databricks Pernsonal Access Token**,
9. Enable *I acknowlegde...* in the bottom of the page
10. click **Create stack**

---

### 4️⃣ Create a Pipeline and Medals Layers folders.
Jobs and Pipeline are both used to automated data processing, but they serve slightly different purpose

**Jobs** is a way to run tasks automatically (like notebook, scripts, or queries).
- What a Job can do:
  - Run a notebook or Python script
  - Execute SQL queries
  - Schedule runs (daily, hourly, etc.)
  - Chain multiple task (workflow / DAG)    

**Pipeline** is a declarative way to build data pipelines.
- You define *what* should happen, not how to run it.
- What a Pipeline does:
  - Automatically manages data flow
  - Handles dependencies between tables
  - Tracks data lineage
  - Supportes streaming + batch
  - Enforces data quality rules 

#### **👷Create a Pipeline and folders:**
  1. Click **Jobs & Pipelines**
  2. Click **ETL pipeline**
  3. Enter a name for pipeline
  4. In **workspace** (**workspace**.default) select the project catalog name
  5. In **default** (workspace.**default**) select the bronze schema
  6. Enable Lakeflow Pipeline Editor
  7. Click **Start with an empty file**
  8. Click **Browse** (enable python), Select the project folder, and click **Select**
  9. It will create a folder named transformation. Inside it create 3 folder (bronze, silver and gold)    

---

### 5️⃣ City Table: Bronze Layers
1. Drag and Drop my_transformation.py file into the bronze folder.
2. Rename the file to city.py
3. Insert into the city.py this [code](./transportation_pipeline/transformations/bronze/city.py)
4. Click **Dry run**: It Check all definitions, types and references in this pipeline without creating or updating any tables.
5. If everthing ok, click **Run Pipeline**

#### 🟩 Code Summary

This code defines a Bronze layer pipeline using Databricks and Apache Spark to ingest raw city data from CSV files. It reads the source data, automatically infers the schema, and safely handles malformed records. During ingestion, it enriches the dataset by adding metadata columns such as the source file path and the ingestion timestamp. The processed data is then stored as a Delta table using a materialized_view, which ensures the data is physically persisted, automatically updated, and optimized for performance. Additional table properties enable change tracking (Change Data Feed) and improve storage efficiency through optimized writes and auto-compaction. Overall, this code implements a scalable and reliable raw data ingestion step (Bronze layer), preparing the data for further transformations in downstream layers (Silver and Gold).


---

## 🚀 Getting Started

### Prerequisites

- Databricks workspace access
- Apache Spark cluster running
- Python 3.x environment
- AWS Account (free)
