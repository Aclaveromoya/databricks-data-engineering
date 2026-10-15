# Databricks notebook source
# MAGIC %md
# MAGIC ## Fase 4 · Historial de clientes 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev_casopractico.gold.dim_clientes_manual (
# MAGIC     cliente_id STRING,
# MAGIC     nombre STRING,
# MAGIC     email STRING,
# MAGIC     fecha_inicio TIMESTAMP,
# MAGIC     fecha_fin TIMESTAMP,
# MAGIC     es_actual BOOLEAN
# MAGIC )
# MAGIC CLUSTER BY (cliente_id);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cierre de registros antiguos e inserción de nuevos

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp

# 1. Leer las novedades limpias que vienen de tu tabla Silver tradicional
df_silver_updates = spark.read.table("dev_casopractico.silver.clientes")

# 2. Filtrar solo registros nuevos o con cambios reales frente al estado actual
df_current = (spark.read.table("dev_casopractico.gold.dim_clientes_manual")
              .filter(col("es_actual") == True))

df_changes = (df_silver_updates.alias("src")
    .join(df_current.alias("tgt"),
          col("src.cliente_id") == col("tgt.cliente_id"),
          "left")
    .filter(
        col("tgt.cliente_id").isNull()              # Cliente nuevo
        | (col("src.nombre") != col("tgt.nombre"))   # Nombre cambió
        | (col("src.email") != col("tgt.email"))     # Email cambió
    )
    .select(col("src.cliente_id"), col("src.nombre"), col("src.email")))

# 3. Duplicamos registros para permitir UPDATE e INSERT en un solo paso
df_updates_with_key = df_changes.withColumn("merge_key", col("cliente_id"))
df_updates_blank_key = df_changes.withColumn("merge_key", lit(None))
df_staging = df_updates_with_key.union(df_updates_blank_key)

# 4. Cargar la tabla destino
delta_target = DeltaTable.forName(spark, "dev_casopractico.gold.dim_clientes_manual")

# 5. Operación MERGE coordinada
(delta_target.alias("target")
 .merge(
     df_staging.alias("source"), 
     "target.cliente_id = source.merge_key AND target.es_actual = true"
 )
 .whenMatchedUpdate(
     condition = "target.nombre != source.nombre OR target.email != source.email",
     set = {
         "fecha_fin": current_timestamp(), 
         "es_actual": "false"
     }
 )
 .whenNotMatchedInsert(
     condition = "source.merge_key IS NULL",
     values = {
         "cliente_id": "source.cliente_id",
         "nombre": "source.nombre",
         "email": "source.email",
         "fecha_inicio": current_timestamp(),
         "fecha_fin": "CAST(NULL AS TIMESTAMP)",
         "es_actual": "true"
     }
 )
 .execute())

print("Forma 2: MERGE manual ejecutado con éxito.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobar resultados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Lakeflow' as metodo, cliente_id, email, __END_AT IS NULL as es_actual, __START_AT as fecha_inicio, __END_AT as fecha_fin 
# MAGIC FROM dev_casopractico.silver.dim_clientes_lakeflow WHERE cliente_id = 'C002'
# MAGIC UNION ALL
# MAGIC SELECT 'Manual_Merge' as metodo, cliente_id, email, es_actual, fecha_inicio, fecha_fin 
# MAGIC FROM dev_casopractico.gold.dim_clientes_manual WHERE cliente_id = 'C002';
# MAGIC