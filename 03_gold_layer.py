# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer
# MAGIC
# MAGIC This notebook creates the customer and product dimension views and builds the purchase fact table for reporting.
# MAGIC

# COMMAND ----------

CATALOG = "sales_store"
SCHEMA = "linio"

SILVER_PURCHASES_TABLE = f"{CATALOG}.{SCHEMA}.silver_compras"
SILVER_CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.silver_clientes"
SILVER_PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.silver_productos"
SILVER_DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.silver_detalles"

GOLD_CUSTOMERS_VIEW = f"{CATALOG}.{SCHEMA}.gold_dim_clientes"
GOLD_PRODUCTS_VIEW = f"{CATALOG}.{SCHEMA}.gold_dim_productos"
GOLD_FACT_TABLE = f"{CATALOG}.{SCHEMA}.gold_fact_compras"


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Gold dimension views
# MAGIC
# MAGIC The lineage timestamp is excluded, as required by the assignment.
# MAGIC

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_CUSTOMERS_VIEW} AS
SELECT
    cliente_id,
    tipo_documento,
    num_documento,
    nombre_completo,
    tipo_cliente
FROM {SILVER_CUSTOMERS_TABLE}
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_PRODUCTS_VIEW} AS
SELECT
    producto_id,
    producto,
    categoria,
    subcategoria
FROM {SILVER_PRODUCTS_TABLE}
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the purchase fact table
# MAGIC
# MAGIC An inner join retains only purchases with a matching invoice in the detail data.
# MAGIC

# COMMAND ----------

df_compras = spark.table(SILVER_PURCHASES_TABLE)
df_detalles = spark.table(SILVER_DETAILS_TABLE)

df_fact_compras = (
    df_compras.alias("p")
    .join(df_detalles.alias("d"), on="factura", how="inner")
    .select(
        col("p.periodo"),
        col("p.venta_id"),
        col("factura"),
        col("p.tipo_compra"),
        col("p.fecha_orden"),
        col("p.fecha_entrega"),
        col("p.fecha_envio"),
        col("p.estado"),
        col("p.cliente_id"),
        col("p.vendedor"),
        col("p.departamento"),
        col("p.metodo_pago"),
        col("p.grupo_dias_envio"),
        col("d.detalle_id"),
        col("d.producto_id"),
        col("d.unidades"),
        col("d.subtotal"),
    )
    .withColumn("fecha_actualizacion", current_timestamp())
)


# COMMAND ----------

(
    df_fact_compras.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("periodo")
    .saveAsTable(GOLD_FACT_TABLE)
)

df_fact_compras.printSchema()
display(df_fact_compras.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validation
# MAGIC

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(DISTINCT factura) AS invoice_count,
    ROUND(SUM(subtotal), 2) AS total_sales
FROM {GOLD_FACT_TABLE}
"""))
