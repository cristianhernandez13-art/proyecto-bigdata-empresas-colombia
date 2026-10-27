
# Importa SparkSession para crear la sesión de trabajo de Apache Spark
from pyspark.sql import SparkSession

# Importa las funciones count para contar registros y desc para ordenar de mayor a menor
from pyspark.sql.functions import count, desc


# Configura e inicia la sesión de Spark para el análisis de empresas
spark = (
    SparkSession.builder
    # Define el nombre de la aplicación
    .appName("AnalisisEmpresasColombia")
    # Ejecuta Spark localmente utilizando los núcleos disponibles del procesador
    .master("local[*]")
    # Crea una sesión nueva o reutiliza una existente
    .getOrCreate()
)

# Configura los mensajes de Spark para mostrar principalmente los errores
spark.sparkContext.setLogLevel("ERROR")


# Lee el archivo CSV que contiene los registros de las empresas
df = (
    spark.read
    # Indica que la primera fila del archivo contiene los nombres de las columnas
    .option("header", True)
    # Permite identificar automáticamente los tipos de datos de las columnas
    .option("inferSchema", True)
    # Carga el archivo empresas.csv en un DataFrame
    .csv("empresas.csv")
)


# Imprime el título del análisis y calcula el total de registros
print("=== ANALISIS BATCH DE EMPRESAS ===")
print("Total de registros:", df.count())


# Muestra los nombres y los tipos de datos de las columnas del archivo
print("=== ESTRUCTURA DEL CSV ===")
df.printSchema()


# Muestra los primeros 10 registros del archivo sin recortar el contenido
print("=== MUESTRA DE EMPRESAS ===")
df.show(10, truncate=False)


# Agrupa los registros según el departamento
print("=== EMPRESAS POR DEPARTAMENTO ===")
df.groupBy("departamento") \
    .agg(count("*").alias("cantidad_empresas")) \
    .orderBy(desc("cantidad_empresas")) \
    .show(truncate=False)

# La consulta anterior cuenta las empresas de cada departamento
# y ordena los resultados desde la mayor hasta la menor cantidad.


# Agrupa los registros según la actividad económica
print("=== EMPRESAS POR ACTIVIDAD ECONOMICA ===")
df.groupBy("actividad_economica") \
    .agg(count("*").alias("cantidad_empresas")) \
    .orderBy(desc("cantidad_empresas")) \
    .show(truncate=False)

# Esta consulta permite identificar las actividades económicas
# con mayor y menor cantidad de registros.


# Agrupa los registros según el tamaño de la empresa
print("=== EMPRESAS POR TAMANO ===")
df.groupBy("tamano_empresa") \
    .agg(count("*").alias("cantidad_empresas")) \
    .orderBy(desc("cantidad_empresas")) \
    .show(truncate=False)

# Esta consulta cuenta las empresas por categoría de tamaño
# y ordena los resultados de mayor a menor cantidad.


# Cierra la sesión de Spark y libera los recursos utilizados
spark.stop()
