import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# 1. Configurar una sesión de Spark local ligera exclusiva para el contenedor de GitHub Actions
@pytest.fixture(scope="session")
def spark_session():
    return (SparkSession.builder
            .appName("databricks-unit-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate())

# 2. La función de transformación que limpia los IDs rotos
def limpiar_clientes_invalidos(df):
    return df.filter("cliente_id != 'ERROR_ROW' AND cliente_id IS NOT NULL")

# 3. El test unitario encargado de validar la lógica
def test_clean_invalid_rows(spark_session):
    schema = StructType([StructField("cliente_id", StringType(), True)])
    data = [("C001",), ("ERROR_ROW",), (None,)]
    df_prueba = spark_session.createDataFrame(data, schema)
    
    df_resultado = limpiar_clientes_invalidos(df_prueba)
    
    # Validaciones obligatorias de pytest
    assert df_resultado.count() == 1
    assert df_resultado.collect()[0]["cliente_id"] == "C001"
