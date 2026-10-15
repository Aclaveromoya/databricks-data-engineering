# 🏪 Caso Práctico: Arquitectura Lakehouse Medallion end to end en Databricks

Este repositorio contiene la implementación completa, gobernada y automatizada de un **Lakehouse corporativo** para una cadena de tiendas retail. El proyecto ha sido desarrollado bajo metodologías de producción (**DataOps**), utilizando estructuras de tres niveles en **Unity Catalog** y desplegando tanto pipelines imperativos como declarativos.

---

## 🗺️ 1. Arquitectura del Sistema (Ecosistema Medallion)

El diseño procesa flujos de clientes y transacciones de tiendas distribuidas a través de tres capas lógicas de almacenamiento gobernadas de forma centralizada:

1. **Capa Bronze (Ingesta Incremental):** Captura de archivos CSV crudos desde un volumen gestionado (`raw`) mediante **Auto Loader (`cloudFiles`)** con un trigger síncrono `availableNow=True`. Preserva los datos malformados de origen para auditoría y mantiene el estado de ingesta mediante checkpoints (`offsets`, `commits`, `sources`).
2. **Capa Silver (Calidad y Enriquecimiento):** Aplicación de tipado estricto, eliminación de duplicados y aislamiento de registros corruptos a través de pipelines declarativos (**Lakeflow Quality Expectations**). Aplicación incremental de novedades de negocio mediante sentencias `MERGE`.
3. **Capa Gold (Modelo Analítico e Histórico):** Creación de dimensiones maestras con tracking de cambios históricos (**SCD Tipo 2**) y vistas materializadas agregadas mensualmente para consumo de analítica avanzada e IA (Genie Agent).

---

## 📈 2. Métricas Reales y Resultados de Rendimiento

A lo largo de las fases de desarrollo y estrés del clúster, se han extraído las siguientes métricas de producción:

### Capa Bronze (Volumetría Incremental)
*   **Batch Inicial (Días 1-2):** Ventas = 15 filas | Clientes = 5 filas.
*   **Batch Incremental (Día 3):** Ventas = 17 filas. El sistema demostró su naturaleza incremental: **Auto Loader ignoró las 15 filas previas** de forma segura gracias al diario de control del checkpoint, absorbiendo únicamente los datos nuevos del Día 3.

### Capa Silver (Optimización e Historial)
*   **Mantenimiento Delta:** El comando `OPTIMIZE` consolidó la fragmentación de archivos Parquet. Capacidad de **Time Travel** validada restaurando estados mediante `RESTORE TO VERSION AS OF x`.
*   **Calidad de Datos (Lakeflow):** El pipeline aisló y descartó exactamente **1 fila corrupta** (registro con monto alfanumérico `"CIEN_EUROS"`) mediante la directiva estricta `@dlt.expect_all_or_drop`.

### Pruebas de Rendimiento Extremo (Mitigación de Skew y Escalabilidad)
*   **Join Tradicional vs Broadcast Join (20M Filas):** Ante un escenario de sesgo masivo (*Data Skew* provocado adrede donde un solo cliente acapara el 30% de las ventas), el **Broadcast Join** redujo el tiempo de procesamiento de **0.95s a 0.84s** al eliminar el intercambio de datos por red (*Shuffle*).
*   **La Paradoja de Escalas Medias (20M Filas):** En 20M de filas, la tabla plana superó a la indexada (**0.66s vs 1.14s**) debido al I/O overhead de apertura de múltiples micro-ficheros en volumetrías que caben holgadamente en la caché de Spark.
*   **Potencial Absoluto de Liquid Clustering (100M Filas):** Al escalar el estrés a **100 millones de registros**, el escaneo secuencial de la tabla plana sufrió degradación (**2.73s**), mientras que la tabla estructurada con `CLUSTER BY` consolidó su estrategia de **Data Skipping**, recortando el tiempo de respuesta a casi la mitad (**1.40s**) al leer únicamente el bloque estadístico indexado.

---

## 🔒 3. Políticas de Gobierno y Seguridad (Unity Catalog)

Se ha implementado seguridad granular basada en roles (RBAC) directamente sobre los objetos lógicos de Unity Catalog sin penalizar el rendimiento del motor:
*   **Column Masking (PII):** Los correos de los clientes se anonimizan dinámicamente con la etiqueta `REDACTED_EMAIL_PII` si el consumidor no pertenece al rol de seguridad autorizado (`pii_readers`).
*   **Row Filtering (Aislamiento Geográfico):** Los analistas regionales ven restringido su espectro de análisis de forma transparente, visualizando exclusivamente los registros vinculados a su tienda autorizada (`TIENDA_MADRID_01`).
*   **Enriquecimiento AI/BI:** Se inyectaron metadatos semánticos mediante `COMMENT ON TABLE`. Esto permite al **Genie Agent** interpretar preguntas en lenguaje natural (ej. *"¿Cuánto facturamos este mes?"*) y traducirlas de forma autónoma a queries SQL corporativas.

---

## 🔄 4. Orquestación y Resiliencia en Producción (DataOps)

Se diseñó un **Lakeflow Job** secuencial con alertas automáticas por fallo (`on-failure`) y políticas de reintento (`3 retries` en la etapa de validación). 
*   Se puso a prueba la tolerancia a fallos provocando una división por cero (`1/0`) controlada.
*   Se ejecutó con éxito la funcionalidad de **Repair Run**, demostrando la capacidad de corregir el script en caliente y relanzar **únicamente la tarea fallida**, optimizando tiempos y costes de cómputo en la nube.

---

## 🛠️ 5. Framework de Testing y Automatización Declarativa (DABs)

*   **CI/CD con GitHub Actions:** Cada actualización en el repositorio dispara un flujo de integración continua que levanta un entorno con Java 17, instala PySpark y ejecuta tests unitarios con `pytest` de forma aislada.
*   **Databricks Asset Bundles (DABs):** Toda la infraestructura del pipeline y la definición de los Jobs se gestionan como código a través del archivo `databricks.yml`, permitiendo desplegar de manera unificada y determinista en entornos independientes:
    ```bash
    databricks bundle deploy -t dev   # Escribe en dev_casopractico_silver_dev
    databricks bundle deploy -t prod  # Escribe en dev_casopractico_silver_prod
    ```

---

## ☁️ 6. Estrategia de Migración y Diseño Arquitectónico en Azure

Para escalar este caso práctico a un despliegue corporativo e industrial dentro del ecosistema de **Microsoft Azure**, se plantea la sustitución de los componentes locales por la siguiente arquitectura SaaS gobernada:

1. **Capa de Almacenamiento (Azure Data Lake Storage Gen2):** Sustituye a los volúmenes locales. Los archivos Parquet y los logs Delta se almacenan en contenedores blob jerárquicos cifrados, aprovisionados mediante **Terraform** (`azurerm_storage_account`).
2. **Autenticación sin Llaves (Azure Access Connector):** Se crea un conector de acceso de Databricks en Azure asociado a una **Managed Identity** (Identidad Administrada) mediante **Terraform** (`azurerm_databricks_access_connector`). Esto evita el intercambio y exposición de llaves rígidas (*Keyless Authentication*).
3. **Mapeo de Datos (Storage Credentials & External Locations):** Objetos de Unity Catalog creados vía SQL para autorizar y enlazar la ruta física de los contenedores de Azure con los esquemas lógicos del Lakehouse.
4. **Seguridad de Secretos (Azure Key Vault):** Mapeo de tokens y variables sensibles mediante un **Secret Scope** de Databricks respaldado de forma opaca por el Key Vault corporativo (`azurerm_key_vault`).
5. **Despliegue Desatendido (Service Principal):** Registro de una aplicación en Microsoft Entra ID (Azure AD) con permisos de colaborador en el Workspace, cuyas credenciales se inyectan en los secretos de GitHub Actions para permitir la ejecución autónoma de `databricks bundle deploy` en producción.
