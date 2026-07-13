# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer
# MAGIC
# MAGIC This notebook ingests the raw purchase and detail files from Azure Data Lake Storage Gen2, standardizes column names, adds lineage columns, and loads the Bronze Delta tables.
# MAGIC

# COMMAND ----------

CATALOG = "sales_store"
SCHEMA = "linio"
CONTAINER_PATH = "abfss://sales-store@stsalesstoresergio.dfs.core.windows.net/"

FILE_PURCHASES_IN_PERSON = CONTAINER_PATH + "landing/compras/Presencial.csv"
FILE_PURCHASES_ONLINE = CONTAINER_PATH + "landing/compras/Online.json"
FILES_DETAILS = CONTAINER_PATH + "landing/detalles/*.csv"

BRONZE_PURCHASES_TABLE = f"{CATALOG}.{SCHEMA}.bronze_compras"
BRONZE_DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_detalles"


# COMMAND ----------

import re

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit
from pyspark.sql.types import StringType, StructField, StructType


# COMMAND ----------

def to_snake_case(column_name: str) -> str:
    """Convert spaces and CamelCase column names to lowercase snake_case."""
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column_name.strip())
    normalized = re.sub(r"[\s\-]+", "_", normalized)
    return normalized.lower()


def normalize_column_names(dataframe):
    """Apply snake_case normalization to every column in a DataFrame."""
    return dataframe.toDF(*[to_snake_case(name) for name in dataframe.columns])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. In-person purchases — CSV
# MAGIC
# MAGIC All source columns are read as strings by disabling schema inference.
# MAGIC

# COMMAND ----------

compras_presencial = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", False)
    .load(FILE_PURCHASES_IN_PERSON)
)

compras_presencial = (
    normalize_column_names(compras_presencial)
    .withColumn("tipo_compra", lit("Presencial"))
    .withColumn("fecha_carga", current_timestamp())
)

compras_presencial.printSchema()
display(compras_presencial.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Online purchases — JSON
# MAGIC
# MAGIC `StructType` and `StringType` explicitly force every expected JSON field to be ingested as text.
# MAGIC

# COMMAND ----------

# Read once only to capture the real JSON field names
online_raw = (
    spark.read
    .option("multiline", True)
    .json(FILE_PURCHASES_ONLINE)
)

print(online_raw.columns)

# Force every real JSON field to StringType
online_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in online_raw.columns
])

compras_online = (
    spark.read
    .format("json")
    .schema(online_schema)
    .option("multiline", True)
    .load(FILE_PURCHASES_ONLINE)
)

compras_online = (
    normalize_column_names(compras_online)
    .withColumn("tipo_compra", lit("Online"))
    .withColumn("fecha_carga", current_timestamp())
)

compras_online.printSchema()
display(compras_online.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consolidate purchase channels
# MAGIC
# MAGIC `unionByName(..., allowMissingColumns=True)` preserves both shared and channel-specific columns.
# MAGIC

# COMMAND ----------

df_compras = compras_presencial.unionByName(
    compras_online,
    allowMissingColumns=True
)

display(df_compras.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Purchase details — bulk CSV ingestion
# MAGIC
# MAGIC All files in `landing/detalles` are ingested in one operation. The source filename and load timestamp provide traceability.
# MAGIC

# COMMAND ----------

df_detalles = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", "|")
    .option("inferSchema", False)
    .option("mergeSchema", True)
    .load(FILES_DETAILS)
)

df_detalles = normalize_column_names(df_detalles)

if "oferta_id" not in df_detalles.columns:
    df_detalles = df_detalles.withColumn(
        "oferta_id",
        lit(None).cast("string")
    )

df_detalles = (
    df_detalles
    .withColumn("nombre_archivo", col("_metadata.file_name"))
    .withColumn("fecha_carga", current_timestamp())
)

df_detalles.printSchema()
display(df_detalles.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Bronze Delta tables
# MAGIC
# MAGIC The assignment requires overwrite loads for both Bronze tables.
# MAGIC

# COMMAND ----------

(
    df_compras.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_PURCHASES_TABLE)
)

(
    df_detalles.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_DETAILS_TABLE)
)

print(f"Loaded: {BRONZE_PURCHASES_TABLE}")
print(f"Loaded: {BRONZE_DETAILS_TABLE}")


# COMMAND ----------

display(spark.sql(f"""
SELECT
    '{BRONZE_PURCHASES_TABLE}' AS table_name,
    COUNT(*) AS row_count
FROM {BRONZE_PURCHASES_TABLE}
UNION ALL
SELECT
    '{BRONZE_DETAILS_TABLE}' AS table_name,
    COUNT(*) AS row_count
FROM {BRONZE_DETAILS_TABLE}
"""))
