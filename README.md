# Project Title: End-to-End Transportation Data Engineering Pipeline
This project demonstrates an enterprise-grade data engineering workflow built using Databricks and LakeFlow Spark Declarative Pipelines (SDP). The primary goal was to modernize a legacy procedural pipeline for a transportation company (Good Cabs) to improve data delivery speeds and regional analytical capabilities.

## Key Features & Technologies
- Declarative Pipelines (SDP): Transitioned from manual, imperative orchestration to a declarative approach, allowing the system to handle execution plans, dependencies, and incremental processing automatically.

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

## 📁 Project Structure
```
Project_transportation / 
  |-- transportation_pipeline/
  |-- README.md
  |-- Project documentation 
  |-- project_setup.ipynb # Initial setup and configuration 
  |__ .gitignore # Git ignore rules
```

## 🚀 Getting Started

### Prerequisites

- Databricks workspace access
- Apache Spark cluster running
- Python 3.x environment

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/AdrianoR85/Project_transportation.git
   cd Project_transportation
