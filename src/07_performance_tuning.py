# Databricks notebook source
# MAGIC %md
# MAGIC ## Fase 7 · Rendimiento

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generar un Tablón de Ventas Pesado con Data Skew

# COMMAND ----------

from pyspark.sql.functions import col, expr, when

print("Generando 20 millones de registros de ventas con sesgo de datos (Data Skew)...")

# Generamos 20 millones de ventas analíticas en memoria sin penalizar costes
df_large_ventas = spark.range(0, 20000000).withColumn(
    "cliente_id",
    # El 30% de los registros completos pertenecerán al cliente 'C_SKEWED' (id terminado en 0, 1 o 2)
    when(col("id") % 10 < 3, "C_SKEWED")
    .otherwise(expr("concat('C_', cast(floor(rand() * 10000) as string))"))
).withColumn("monto", expr("rand() * 100"))

# Guardamos dos copias para los experimentos del guion
# Copia A: Sin indexar (Tabla Plana)
df_large_ventas.write.mode("overwrite").saveAsTable("dev_casopractico.silver.ventas_heavy_plana")

# Copia B: Con Liquid Clustering por cliente (Punto 4 del guion)
(df_large_ventas.write
 .mode("overwrite")
 .clusterBy("cliente_id")
 .saveAsTable("dev_casopractico.silver.ventas_heavy_clustered"))

# Optimizar la tabla indexada para consolidar el Liquid Clustering
spark.sql("OPTIMIZE dev_casopractico.silver.ventas_heavy_clustered")

print("Tablas de rendimiento inicializadas con éxito en la capa Silver.")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dev_casopractico.silver.ventas_heavy_clustered

# COMMAND ----------

# MAGIC %md
# MAGIC ### 
# MAGIC Test A: Join Estándar (Genera Shuffle masivo)
# MAGIC

# COMMAND ----------

import time

# Forzar lectura limpia de la tabla plana
df_ventas_plana = spark.read.table("dev_casopractico.silver.ventas_heavy_plana")
df_clientes = spark.read.table("dev_casopractico.gold.dim_clientes_manual")

start_time = time.time()

# Join tradicional con agregación por cliente
df_join_estandar = df_ventas_plana.join(df_clientes, "cliente_id").groupBy("cliente_id").count()

# Usamos noop (no-operation) para medir el tiempo real de cómputo en memoria sin coste de escritura física
df_join_estandar.write.mode("noop").format("noop").save()

tiempo_estandar = time.time() - start_time
print(f"Tiempo Test A (Join Estándar): {tiempo_estandar:.2f} segundos.")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Test B: Broadcast Join (Elimina el Shuffle)
# MAGIC

# COMMAND ----------

import time

start_time = time.time()

# Forzar Broadcast Join sobre la tabla pequeña (clientes) para mitigar el Data Skew
df_join_broadcast = df_ventas_plana.join(df_clientes.hint("broadcast"), "cliente_id").groupBy("cliente_id").count()

# Forzamos el cálculo
total_filas_B = df_join_broadcast.count()

tiempo_broadcast = time.time() - start_time
print(f"Resultado Filas: {total_filas_B}")
print(f"Tiempo Test B (Broadcast Join): {tiempo_broadcast:.2f} segundos.")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Test C: Filtrado en Tabla Plana

# COMMAND ----------

import time

start_time = time.time()

df_filtro_plano = spark.read.table("dev_casopractico.silver.ventas_heavy_plana").filter("cliente_id = 'C_SKEWED'")
total_filtro_A = df_filtro_plano.count()

tiempo_filtro_plano = time.time() - start_time
print(f"Tiempo Filtrado en Tabla Plana: {tiempo_filtro_plano:.2f} segundos.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Test D: Filtrado con Liquid Clustering (Data Skipping)
# MAGIC

# COMMAND ----------

import time

start_time = time.time()

df_filtro_clustered = spark.read.table("dev_casopractico.silver.ventas_heavy_clustered").filter("cliente_id = 'C_SKEWED'")
total_filtro_B = df_filtro_clustered.count()

tiempo_filtro_clustered = time.time() - start_time
print(f"Tiempo Filtrado con Liquid Clustering: {tiempo_filtro_clustered:.2f} segundos.")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Generacion de tabla con mil millones de registros

# COMMAND ----------

from pyspark.sql.functions import col, expr, when
import time

print("Generando 1000 millones de registros para estresar el Liquid Clustering...")

# 1. Generar 100 millones de filas
df_ultra_ventas = spark.range(0, 1000000000).withColumn(
    "cliente_id",
    # Creamos un cliente único 'C_AGUJA' en la última fila, el resto distribuidos
    when(col("id") == 999999999, "C_AGUJA")
    .otherwise(expr("concat('C_', cast(floor(rand() * 100000) as string))"))
).withColumn("monto", expr("rand() * 100"))

# 2. Guardar versión plana (Sin Indexar)
df_ultra_ventas.write.mode("overwrite").saveAsTable("dev_casopractico.silver.ventas_1000M_plana")

# 3. Guardar versión con Liquid Clustering por cliente
(df_ultra_ventas.write
 .mode("overwrite")
 .clusterBy("cliente_id")
 .saveAsTable("dev_casopractico.silver.ventas_1000M_clustered"))

# 4. Consolidar el índice físicamente en disco
spark.sql("OPTIMIZE dev_casopractico.silver.ventas_1000M_clustered")

print("¡Tablas de 1000 Millones inicializadas y optimizadas con éxito!")


# COMMAND ----------

start_time = time.time()

# Spark tiene que abrir y leer cada mega de la tabla para ver si está el ID
total_plana = spark.read.table("dev_casopractico.silver.ventas_1000M_plana").filter("cliente_id = 'C_AGUJA'").count()

print(f"Tiempo Tabla Plana (1000M): {time.time() - start_time:.2f} segundos. Filas encontradas: {total_plana}")


# COMMAND ----------

start_time = time.time()

# Spark mirará los metadatos, sabrá en qué archivo exacto está 'C_AGUJA' y saltará el resto
total_clustered = spark.read.table("dev_casopractico.silver.ventas_1000M_clustered").filter("cliente_id = 'C_AGUJA'").count()

print(f"Tiempo Liquid Clustering (100M): {time.time() - start_time:.2f} segundos. Filas encontradas: {total_clustered}")
