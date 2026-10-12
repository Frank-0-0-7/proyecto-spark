import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round

print("\n--------------------------------")
print("Iniciando pySpark...")
print("--------------------------------\n")

# 1. Preparar el entorno y cargar datos
spark = SparkSession.builder \
    .appName("Analisis_de_datos_TP5") \
    .getOrCreate()

# Comando para limpiar logs
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/opt/spark/data/e-commerce_orders.csv", header=True, inferSchema=True, sep=";")

# 2. Exploración y limpieza

# a. Visualizar las primeras filas.
df.show(5)

# b. Validar si existen registros nulos o inconsistentes y eliminarlos
df_limpio = df.dropna()

# c. Asegurar que los tipos de dato sean correctos (por ejemplo: Price como float)
df_limpio = df_limpio.withColumn("Price", col("Price").cast("float"))

df_limpio.printSchema()


# 3. Transformaciones y métricas

# a. Calcular el monto total y promedio de ventas. (con redondeo a 2 decimales)
monto_total_y_promedio = df_limpio.agg(
    round(sum("Price"), 2).cast("decimal(18, 2)").alias("Monto_Total"),
    round(avg("Price"), 2).cast("decimal(18, 2)").alias("Promedio")
)
monto_total_y_promedio.show()

# b. Contar la cantidad de pedidos por estado (Ejecutado, Pendiente o Suspendido).
pedidos_por_estado = df_limpio.groupBy("status").count().withColumnRenamed("count", "Cantidad")
pedidos_por_estado.show()

# c. Obtener el top 3 de clientes que más dinero gastaron.
top_clientes = df_limpio.groupBy("customer_id") \
    .agg(round(sum("Price"), 2).cast("decimal(18, 2)").alias("Total_Gastado")) \
    .orderBy(col("Total_Gastado").desc()) \
    .limit(3)
top_clientes.show()

# Pausa para inspeccionar la interfaz web de Spark antes de finalizar
print("\n\n[INFO] Pausa de 120 segundos para monitoreo...\n")
print("Spark UI: http://localhost:4040")
print("Cluster UI: http://localhost:8080")

time.sleep(120)

# Detenemos el cluster
spark.stop()
print("\n---  Spark detenido  ---\n")