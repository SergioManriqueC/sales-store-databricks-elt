# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer
# MAGIC
# MAGIC This notebook cleans and transforms the Bronze data into analytics-ready purchase, customer, product, and detail Delta tables.
# MAGIC

# COMMAND ----------

CATALOG = "sales_store"
SCHEMA = "linio"

BRONZE_PURCHASES_TABLE = f"{CATALOG}.{SCHEMA}.bronze_compras"
BRONZE_DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_detalles"
SILVER_PURCHASES_TABLE = f"{CATALOG}.{SCHEMA}.silver_compras"
SILVER_CUSTOMERS_TABLE = f"{CATALOG}.{SCHEMA}.silver_clientes"
SILVER_PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.silver_productos"
SILVER_DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.silver_detalles"


# COMMAND ----------

from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    datediff,
    initcap,
    length,
    lit,
    lpad,
    row_number,
    split,
    substring,
    substring_index,
    to_date,
    trim,
    trunc,
    upper,
    when,
)
from pyspark.sql.window import Window


# COMMAND ----------

def parse_date(column_name):
    value = trim(col(column_name))

    value = when(
        value.isNull() | value.isin("NaN", '"NaN"', ""),
        None
    ).otherwise(value)

    return coalesce(
        to_date(value, "yyyy-MM-dd"),
        to_date(value, "dd/MM/yyyy"),
        to_date(value, "dd-MM-yy"),
        to_date(value, "dd-MM-yyyy"),
        to_date(value, "MM/dd/yyyy")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean and transform purchases
# MAGIC

# COMMAND ----------

df_compras_import = (
    spark.table(BRONZE_PURCHASES_TABLE)
    .select(
        "venta_id",
        "factura",
        "fecha_orden",
        "fecha_entrega",
        "fecha_envio",
        "estado",
        "cliente_code",
        "tipo_cliente",
        "nombres",
        "apellidos",
        "vendedor",
        "departamento",
        "metodo_pago",
        "tipo_compra",
    )
)


# COMMAND ----------

df_compras_clean = (
    df_compras_import
    .withColumn("venta_id", col("venta_id").cast("integer"))
    .withColumn("estado", col("estado").cast("integer"))
    .withColumn("fecha_orden", to_date(trim(col("fecha_orden")), "yyyy-MM-dd"))
    .withColumn("fecha_entrega", to_date(trim(col("fecha_entrega")), "dd/MM/yyyy"))
    .withColumn(
        "fecha_envio",
        to_date(
            when(
                trim(col("fecha_envio")).isin("NaN", '"NaN"', ""),
                None,
            ).otherwise(trim(col("fecha_envio"))),
            "dd-MM-yy",
        ),
    )
    .withColumn("factura", upper(trim(col("factura"))))
    .withColumn("vendedor", trim(col("vendedor")))
    .withColumn("departamento", trim(col("departamento")))
    .withColumn("metodo_pago", trim(col("metodo_pago")))
)


# COMMAND ----------

display(
    df_compras_clean.select(
        "tipo_compra",
        "fecha_orden",
        "fecha_entrega",
        "fecha_envio"
    ).limit(30)
)

# COMMAND ----------

df_compras_transform = (
    df_compras_clean
    .withColumn(
        "cliente_id",
        substring_index(col("cliente_code"), "-", 1).cast("integer"),
    )
    .withColumn(
        "num_documento",
        substring_index(col("cliente_code"), "-", -1).cast("string"),
    )
    .withColumn(
        "vendedor",
        when(
            col("vendedor").isNull() | (trim(col("vendedor")) == ""),
            lit("No Identificado"),
        ).otherwise(col("vendedor")),
    )
    .withColumn(
        "dias_envio",
        when(
            col("estado") == 5,
            datediff(col("fecha_envio"), col("fecha_orden")),
        ).otherwise(None),
    )
    .withColumn(
        "grupo_dias_envio",
        when(col("dias_envio").isNull(), None)
        .when(col("dias_envio") <= 3, lit("[0 - 3 días]"))
        .when(col("dias_envio") <= 7, lit("[4 - 7 días]"))
        .otherwise(lit("[más de 8 días]")),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `silver_compras`
# MAGIC
# MAGIC `periodo` is normalized to the first day of the purchase month and used as the table partition.
# MAGIC

# COMMAND ----------

df_compras = (
    df_compras_transform
    .select(
        "venta_id",
        "factura",
        "tipo_compra",
        "fecha_orden",
        "fecha_entrega",
        "fecha_envio",
        "estado",
        "cliente_id",
        "vendedor",
        "departamento",
        "metodo_pago",
        "dias_envio",
        "grupo_dias_envio",
    )
    .withColumn("periodo", trunc(col("fecha_orden"), "month"))
    .withColumn("fecha_actualizacion", current_timestamp())
)

(
    df_compras.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("periodo")
    .saveAsTable(SILVER_PURCHASES_TABLE)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the customer dimension source
# MAGIC
# MAGIC For each `cliente_id`, the most recent purchase record is retained.
# MAGIC

# COMMAND ----------

customer_window = Window.partitionBy("cliente_id").orderBy(
    col("fecha_orden").desc_nulls_last()
)

df_clientes_transform = (
    df_compras_transform
    .withColumn("row_number", row_number().over(customer_window))
    .filter(col("row_number") == 1)
    .drop("row_number")
    .filter(col("cliente_id").isNotNull())
    .withColumn("num_documento", trim(col("num_documento")))
    .withColumn(
        "num_documento",
        when(
            length(col("num_documento")) < 8,
            lpad(col("num_documento"), 8, "0"),
        ).otherwise(col("num_documento")),
    )
    .withColumn("nombres", initcap(trim(col("nombres"))))
    .withColumn("apellidos", initcap(trim(col("apellidos"))))
    .withColumn("tipo_cliente", upper(trim(col("tipo_cliente"))))
    .withColumn(
        "tipo_documento",
        when(length(col("num_documento")) == 8, lit("DNI"))
        .when(
            (length(col("num_documento")) == 11)
            & (substring(col("num_documento"), 1, 2) == "10"),
            lit("RUC10"),
        )
        .when(
            (length(col("num_documento")) == 11)
            & (substring(col("num_documento"), 1, 2) == "20"),
            lit("RUC20"),
        )
        .otherwise(None),
    )
    .withColumn(
        "nombre_completo",
        concat_ws(" ", col("nombres"), col("apellidos")),
    )
)

df_clientes = (
    df_clientes_transform
    .select(
        "cliente_id",
        "tipo_documento",
        "num_documento",
        "nombre_completo",
        "tipo_cliente",
    )
    .withColumn("fecha_actualizacion", current_timestamp())
)

(
    df_clientes.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_CUSTOMERS_TABLE)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and transform purchase details
# MAGIC

# COMMAND ----------

df_detalles_import = spark.table(BRONZE_DETAILS_TABLE)

df_detalles_clean = (
    df_detalles_import
    .withColumn("detalle_id", col("detalle_id").cast("integer"))
    .withColumn("unidades", col("unidades").cast("integer"))
    .withColumn("oferta_id", col("oferta_id").cast("integer"))
    .withColumn("precio_unitario", col("precio_unitario").cast("double"))
    .withColumn("factura", upper(trim(col("factura"))))
    .withColumn("producto", trim(col("producto")))
)

df_detalles_transform = (
    df_detalles_clean
    .withColumn("subtotal", col("unidades") * col("precio_unitario"))
    .withColumn("tienda", split(col("nombre_archivo"), r"\.")[0])
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `silver_productos`
# MAGIC
# MAGIC Duplicate products are removed. The target table generates `producto_id` automatically.
# MAGIC

# COMMAND ----------

product_window = Window.partitionBy("producto").orderBy(col("producto"))

df_productos = (
    df_detalles_transform
    .withColumn("row_number", row_number().over(product_window))
    .filter(col("row_number") == 1)
    .select(
        trim(col("producto")).alias("producto"),
        upper(trim(col("categoria"))).alias("categoria"),
        upper(trim(col("subcategoria"))).alias("subcategoria"),
    )
    .withColumn("fecha_actualizacion", current_timestamp())
)

# Preserve the identity definition created by DDL.sql.
spark.sql(f"TRUNCATE TABLE {SILVER_PRODUCTS_TABLE}")

(
    df_productos.write.format("delta")
    .mode("append")
    .saveAsTable(SILVER_PRODUCTS_TABLE)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create `silver_detalles`
# MAGIC
# MAGIC The generated product key is added through an inner join with `silver_productos`.
# MAGIC

# COMMAND ----------

df_productos_new = (
    spark.table(SILVER_PRODUCTS_TABLE)
    .select("producto_id", "producto")
)

df_detalles = (
    df_detalles_transform.alias("det")
    .join(df_productos_new.alias("prod"), on="producto", how="inner")
    .select(
        col("det.detalle_id"),
        col("det.factura"),
        col("det.producto"),
        col("prod.producto_id"),
        col("det.unidades"),
        col("det.precio_unitario"),
        col("det.subtotal"),
    )
    .withColumn("fecha_actualizacion", current_timestamp())
)

(
    df_detalles.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_DETAILS_TABLE)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation
# MAGIC

# COMMAND ----------

display(spark.sql(f"""
SELECT '{SILVER_PURCHASES_TABLE}' AS table_name, COUNT(*) AS row_count
FROM {SILVER_PURCHASES_TABLE}
UNION ALL
SELECT '{SILVER_CUSTOMERS_TABLE}', COUNT(*)
FROM {SILVER_CUSTOMERS_TABLE}
UNION ALL
SELECT '{SILVER_PRODUCTS_TABLE}', COUNT(*)
FROM {SILVER_PRODUCTS_TABLE}
UNION ALL
SELECT '{SILVER_DETAILS_TABLE}', COUNT(*)
FROM {SILVER_DETAILS_TABLE}
"""))
