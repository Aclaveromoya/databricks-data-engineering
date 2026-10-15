import dlt
from pyspark.sql.functions import col, current_timestamp, to_date, date_format

# Definición de la ruta origen de tu volumen
RAW_VOL_PATH = "/Volumes/dev_casopractico/bronze/raw"

# =============================================================================
# 1. CAPA BRONZE: Ingesta Streaming con Auto Loader
# =============================================================================
@dlt.table(
    name="ventas_bronze_lf",
    comment="Tabla streaming de ingesta cruda de ventas desde el volumen raw"
)
def ventas_bronze_lf():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{RAW_VOL_PATH}/ventas_*.csv")
    )

# --- NUEVA INGESTA DE CLIENTES EN BRONZE ---
@dlt.table(
    name="clientes_bronze_lf",
    comment="Tabla streaming de ingesta cruda de clientes desde el volumen raw"
)
def clientes_bronze_lf():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{RAW_VOL_PATH}/clientes_*.csv")
        # Añadimos una marca de tiempo para ordenar la secuencia en el CDC de forma segura
        .withColumn("_fecha_secuencia", current_timestamp())
    )

# =============================================================================
# 2. CAPA SILVER: Data Quality con Expectations (Warn y Drop)
# =============================================================================
rules_silver_ventas = {
    "tienda_valida": "tienda_id IS NOT NULL",
    "monto_numerico_valido": "CAST(monto AS DOUBLE) IS NOT NULL"
}

@dlt.table(
    name="ventas_silver_lf",
    comment="Tabla Silver limpia y tipada con control de calidad estricto"
)
@dlt.expect_all_or_drop(rules_silver_ventas) # DROP: elimina físicamente registros corruptos
@dlt.expect("fecha_valida", "fecha_venta IS NOT NULL") # WARN: alerta visual en las métricas
def ventas_silver_lf():
    return (
        dlt.read_stream("ventas_bronze_lf")
        .select(
            col("venta_id").cast("string"),
            col("cliente_id").cast("string"),
            col("tienda_id").cast("string"),
            col("monto").cast("double"),
            to_date(col("fecha")).alias("fecha_venta"),
            current_timestamp().alias("_fecha_procesamiento")
        )
    )
    # --- NUEVA DIMENSIÓN HISTÓRICA DE CLIENTES (AUTO CDC EN SCD TYPE 2) ---
dlt.create_target_table(
    name="dim_clientes_lakeflow",
    comment="Dimensión de clientes histórica gestionada automáticamente con SCD Tipo 2 por Lakeflow"
)

dlt.apply_changes(
    target="dim_clientes_lakeflow",
    source="clientes_bronze_lf",
    keys=["cliente_id"],
    sequence_by=col("_fecha_secuencia"),
    stored_as_scd_type=2,
    # Eliminamos registros corruptos del flujo histórico directamente en el merge de salida
    track_history_column_list=["nombre", "email"]
)

# =============================================================================
# 3. CAPA GOLD: Materialized View (Agregados Analíticos)
# =============================================================================
@dlt.table(
    name="ventas_por_cliente_mes_gold_lf",
    comment="Vista materializada con agregados de ventas mensuales por cliente"
)
def ventas_por_cliente_mes_gold_lf():
    return (
        dlt.read("ventas_silver_lf")
        .withColumn("año_mes", date_format(col("fecha_venta"), "yyyy-MM"))
        .groupBy("cliente_id", "año_mes")
        .agg(
            {"monto": "sum", "venta_id": "count"}
        )
        .withColumnRenamed("sum(monto)", "total_facturado")
        .withColumnRenamed("count(venta_id)", "cantidad_transacciones")
    )
