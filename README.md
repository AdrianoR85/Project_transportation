# Project_transportation

A Databricks-based data pipeline project designed to manage and process transportation data using a medallion architecture (Bronze, Silver, Gold layers).

## 📋 Overview

This project implements a scalable data pipeline on Databricks with a three-layer medallion architecture:
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Business-ready aggregated data

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
