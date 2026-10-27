
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Crear la sesión de Spark
spark = (
    SparkSession.builder
    .appName("EmpresasKafkaStreaming")
    .config(
        "spark.sql.streaming.forceDeleteTempCheckpointLocation",
        "true"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Definir la estructura de los datos de las empresas
esquema = (
    StructType()
    .add("id_registro_simulado", StringType())
    .add("razon_social_simulada", StringType())
    .add("fecha_matricula", StringType())
    .add("tipo_organizacion", StringType())
    .add("actividad_economica", StringType())
    .add("codigo_ciiu", StringType())
    .add("municipio", StringType())
    .add("departamento", StringType())
)

print("Conectando Spark Streaming con Kafka...")

# Leer los mensajes del tópico empresas
datos = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "empresas")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir los mensajes JSON en columnas
empresas = (
    datos
    .selectExpr("CAST(value AS STRING) AS mensaje")
    .select(
        from_json(col("mensaje"), esquema).alias("empresa")
    )
    .select("empresa.*")
)

# Mostrar los registros recibidos en la consola
consulta = (
    empresas.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "C:/Users/LENOVO_THINKCENTRE/Desktop/ProyectoEmpresasColombia/checkpoint_empresas"
    )
    .start()
)

print("Spark Streaming está leyendo los mensajes de Kafka.")

# Mantener la consulta activa
consulta.awaitTermination()
