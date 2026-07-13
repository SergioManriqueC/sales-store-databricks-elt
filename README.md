# Sales Store ELT Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0078D4)
![Databricks](https://img.shields.io/badge/Azure-Databricks-EF3E42)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-blue)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-black)

## Project Overview

This project implements an ELT pipeline for purchase data using Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, Unity Catalog, Databricks Jobs, and GitHub.

The solution follows the Medallion Architecture:

```text
ADLS Gen2
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer
   ↓
Databricks Dashboard
