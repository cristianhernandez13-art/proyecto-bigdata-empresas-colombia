
# Importar KafkaProducer para enviar mensajes a Kafka
from kafka import KafkaProducer

# Importar csv para leer el archivo de empresas
import csv

# Importar json para convertir los registros a formato JSON
import json


# Definir la ruta donde se encuentra el archivo empresas.csv
archivo = r"C:\Users\LENOVO_THINKCENTRE\Desktop\ProyectoEmpresasColombia\empresas.csv"


# Crear el productor y establecer la conexión con el servidor Kafka
productor = KafkaProducer(
    # Indicar la dirección IP y el puerto del servidor Kafka
    bootstrap_servers="127.0.0.1:9092",

    # Convertir cada registro a JSON y codificarlo en UTF-8
    value_serializer=lambda dato: json.dumps(
        dato, ensure_ascii=False
    ).encode("utf-8")
)


# Abrir el archivo CSV en modo lectura
with open(archivo, "r", encoding="utf-8-sig", newline="") as csvfile:

    # Leer cada fila como un diccionario usando los encabezados del archivo
    lector = csv.DictReader(csvfile)

    # Recorrer todos los registros del archivo CSV
    for fila in lector:

        # Enviar cada registro al tópico empresas de Kafka
        productor.send("empresas", fila)


# Esperar a que se envíen todos los mensajes pendientes
productor.flush()

# Cerrar el productor y liberar sus recursos
productor.close()


# Mostrar un mensaje cuando finalice el envío de los registros
print("Envio finalizado: registros enviados al tópico empresas.")
