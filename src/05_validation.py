# Databricks notebook source
# MAGIC %md
# MAGIC ## Fase 5 · Orquestación 

# COMMAND ----------

# Leer los registros vigentes de nuestra dimensión Gold manual
count_actual = spark.read.table("dev_casopractico.gold.dim_clientes_manual").filter("es_actual = true").count()

print(f"Validación: Tenemos {count_actual} clientes activos en la dimensión Gold.")

# ---  PROVOCAR EL FALLO CONTROLADO (Punto 3 del guion) ---
# Descomenta la línea de abajo para probar la alerta y el Repair Run
#  1 / 0
