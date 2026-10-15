# Databricks notebook source
# MAGIC %md
# MAGIC ## Fase 2 · Capa Silver - Clientes

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# 1. Leer de Bronze
df_bronze_clientes = spark.read.table("dev_casopractico.bronze.clientes")

# 2. Transformar y Limpiar (Quitar duplicados y la fila rota 'ERROR_ROW' o nula)
df_silver_clientes = (df_bronze_clientes
    .filter((col("cliente_id") != "ERROR_ROW") & (col("cliente_id").isNotNull()))
    .dropDuplicates(["cliente_id"])
    .select(
        col("cliente_id").cast("string"),
        col("nombre").cast("string"),
        col("email").cast("string"),
        current_timestamp().alias("_fecha_actualizacion")
    )
)

# 3. Crear la tabla Silver con Liquid Clustering (CLUSTER BY) y activar CDF en sus propiedades
(df_silver_clientes.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")  # Punto 5: Activar CDF
    .clusterBy("cliente_id")                       # Punto 4: Liquid Clustering
    .saveAsTable("dev_casopractico.silver.clientes"))

print("Tabla dev_casopractico.silver.clientes creada con éxito.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Aplicación de Cambios con MERGE

# COMMAND ----------

from delta.tables import DeltaTable

# Cargar la tabla Silver como un objeto DeltaTable
target_table = DeltaTable.forName(spark, "dev_casopractico.silver.clientes")

# Leer las novedades de Bronze (en un caso real filtrarías por el lote incremental)
source_updates = spark.read.table("dev_casopractico.bronze.clientes") \
    .filter((col("cliente_id") != "ERROR_ROW") & (col("cliente_id").isNotNull())) \
    .dropDuplicates(["cliente_id"]) # Nos quedamos con la última versión de la clave

# Ejecutar MERGE
(target_table.alias("target")
 .merge(source_updates.alias("source"), "target.cliente_id = source.cliente_id")
 .whenMatchedUpdate(set={
     "nombre": "source.nombre",
     "email": "source.email",
     "_fecha_actualizacion": "current_timestamp()"
 })
 .whenNotMatchedInsert(values={
     "cliente_id": "source.cliente_id",
     "nombre": "source.nombre",
     "email": "source.email",
     "_fecha_actualizacion": "current_timestamp()"
 })
 .execute())

print("MERGE ejecutado exitosamente.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Auditoría, Time Travel y Optimización

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A. Ver el historial de la tabla (apunta las versiones para tu reporte)
# MAGIC DESCRIBE HISTORY dev_casopractico.silver.clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consultar el Data Change Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cliente_id, nombre, email, _change_type, _commit_version 
# MAGIC FROM table_changes('dev_casopractico.silver.clientes', 24, 25);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- C. Optimizar la tabla (Une los pequeños archivos en bloques óptimos)
# MAGIC OPTIMIZE dev_casopractico.silver.clientes;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- D. Medir el estado de archivos DESPUÉS de optimizar (Compara 'numFiles')
# MAGIC DESCRIBE DETAIL dev_casopractico.silver.clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- E. Simulación de Time Travel (Ver cómo estaba la tabla en la versión 0 - Inicial)
# MAGIC SELECT * FROM dev_casopractico.silver.clientes VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- F. Simulación de RESTORE (Volver a la versión inicial eliminando el MERGE temporalmente)
# MAGIC RESTORE TABLE dev_casopractico.silver.clientes TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- G. Eliminar archivos antiguos que ya no pertenecen al historial activo 
# MAGIC VACUUM dev_casopractico.silver.clientes RETAIN 168 HOURS; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Punto 5: Leer las filas que cambiaron (CDF)
# MAGIC SELECT cliente_id, nombre, email, _change_type, _commit_version 
# MAGIC FROM table_changes('dev_casopractico.silver.clientes', 1, 23);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dev_casopractico.silver.clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, operation, operationMetrics 
# MAGIC FROM (DESCRIBE HISTORY dev_casopractico.silver.clientes) 
# MAGIC WHERE operation = 'OPTIMIZE';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Debido a la ausencia de operaciones de optimize debido al pequeño tamaño des nuestros datos, vamos a forzar y crear más particiones pequeñas

# COMMAND ----------

# Forzar a Spark a escribir cada fila en un hilo diferente creando mini-ficheros
from pyspark.sql.functions import col, current_timestamp

df_bronze_clientes = spark.read.table("dev_casopractico.bronze.clientes")
df_silver_clean = (df_bronze_clientes
    .filter("cliente_id != 'ERROR_ROW' AND cliente_id IS NOT NULL")
    .select(
        col("cliente_id").cast("string"),
        col("nombre").cast("string"),
        col("email").cast("string"),
        current_timestamp().alias("_fecha_actualizacion")
    ))

# Usamos repartition() con un número alto para obligar a romper los datos en muchos archivos pequeños
df_silver_clean.repartition(10).write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("dev_casopractico.silver.clientes")

print("Tabla re-escrita de forma fragmentada con éxito.")
