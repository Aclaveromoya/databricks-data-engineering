# Databricks notebook source
# MAGIC %md
# MAGIC ##Fase 6 · Gobierno y consumo 

# COMMAND ----------

# MAGIC %md #### Dar permisos de lectura

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conceder accesos en cascada para consumo analítico
# MAGIC GRANT USE CATALOG ON CATALOG dev_casopractico TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA dev_casopractico.gold TO `account users`;
# MAGIC GRANT SELECT ON SCHEMA dev_casopractico.gold TO `account users`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creamos máscara para columna PII

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Crear la función de enmascaramiento
# MAGIC CREATE OR REPLACE FUNCTION dev_casopractico.gold.email_mask(email STRING)
# MAGIC RETURN CASE 
# MAGIC   WHEN is_account_group_member('pii_readers') THEN email 
# MAGIC   ELSE 'REDACTED_EMAIL_PII' 
# MAGIC END;
# MAGIC
# MAGIC -- 2. Aplicar la máscara a la columna email de tu dimensión Gold manual
# MAGIC ALTER TABLE dev_casopractico.gold.dim_clientes_manual 
# MAGIC ALTER COLUMN email SET MASK dev_casopractico.gold.email_mask;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Crear filtro para ventas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Crear la función que actúa como filtro de fila
# MAGIC CREATE OR REPLACE FUNCTION dev_casopractico.gold.tienda_filter(tienda_id STRING)
# MAGIC RETURN is_account_group_member('sales_all') OR tienda_id = 'TIENDA_MADRID_01';
# MAGIC
# MAGIC -- 2. Aplicar el filtro de fila a tu tabla de ventas de Lakeflow
# MAGIC ALTER TABLE dev_casopractico.silver.ventas_silver_lf 
# MAGIC SET ROW FILTER dev_casopractico.gold.tienda_filter ON (tienda_id);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enriquecimiento Semántico y Optimización

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Añadir comentarios explicativos a nivel de tabla y columna
# MAGIC COMMENT ON TABLE dev_casopractico.gold.dim_clientes_manual IS 'Dimensión maestra de clientes limpios de la cadena con registro histórico SCD Tipo 2';
# MAGIC COMMENT ON COLUMN dev_casopractico.gold.dim_clientes_manual.email IS 'Dirección de correo electrónico protegida bajo políticas de privacidad de datos personales PII';
# MAGIC
# MAGIC -- Aplicar Liquid Clustering para optimizar las consultas y búsquedas analíticas por cliente
# MAGIC ALTER TABLE dev_casopractico.gold.dim_clientes_manual CLUSTER BY (cliente_id);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verificación Criterio Aceptación

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cliente_id, nombre, email FROM dev_casopractico.gold.dim_clientes_manual LIMIT 5;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT tienda_id FROM dev_casopractico.silver.ventas_silver_lf;