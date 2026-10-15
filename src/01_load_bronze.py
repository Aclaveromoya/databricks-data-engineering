# Databricks notebook source
# MAGIC %md
# MAGIC ## Fase 1 · Datos de partida y capa bronze

# COMMAND ----------

# Rutas nativas de los Volúmenes de Unity Catalog
base_raw_path = "/Volumes/dev_casopractico/bronze/raw"
base_checkpoint_path = "/Volumes/dev_casopractico/bronze/checkpoints"

# Diccionario de configuración adaptado a la nomenclatura de 3 niveles
datasets = {
    "clientes": {
        "source": f"{base_raw_path}/clientes_*.csv",
        "checkpoint": f"{base_checkpoint_path}/clientes",
        "table": "dev_casopractico.bronze.clientes"
    },
    "ventas": {
        "source": f"{base_raw_path}/ventas_*.csv",
        "checkpoint": f"{base_checkpoint_path}/ventas",
        "table": "dev_casopractico.bronze.ventas"
    }
}

# Importar funciones de Spark necesarias
from pyspark.sql.functions import col, current_timestamp

for name, paths in datasets.items():
    print(f"Iniciando carga de Auto Loader para: {name}")
    
    # 1. Configurar el lector de Auto Loader (cloudFiles)
    df_stream = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "csv")
                 .option("cloudFiles.schemaLocation", f"{base_checkpoint_path}/schemas/{name}")
                 .option("cloudFiles.inferColumnTypes", "false") # Mantener como string en Bronze
                 .option("header", "true")
                 .load(paths["source"]))
    
    # 2. Añadir columnas de metadatos (Origen e instantánea de carga)
    df_bronze = (df_stream
                 .withColumn("_metadata_file_path", col("_metadata.file_path"))
                 .withColumn("_fecha_carga", current_timestamp()))
    
    # 3. Escritura en formato Delta con Trigger AvailableNow
    query = (df_bronze.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", paths["checkpoint"])
             .trigger(availableNow=True)
             .toTable(paths["table"]))
    
    # Esperar a que el stream termine
    query.awaitTermination()
    print(f"Tabla Delta {paths['table']} actualizada con éxito.")



# COMMAND ----------

# Comprobación de filas finales
print(f"Filas finales en clientes_bronze: {spark.read.table('dev_casopractico.bronze.clientes').count()}")
print(f"Filas finales en ventas_bronze: {spark.read.table('dev_casopractico.bronze.ventas').count()}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_casopractico.bronze.ventas LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_casopractico.bronze.clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL dev_casopractico.bronze.clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED dev_casopractico.bronze.clientes;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES dev_casopractico.bronze.clientes;