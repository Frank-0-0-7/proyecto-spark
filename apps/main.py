from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# -----------------------------------------------------------------------------
# 1. PREPARAR EL ENTORNO Y CARGAR DATOS
# -----------------------------------------------------------------------------

# Inicializamos la sesión de Spark. 
# El 'appName' es el nombre que aparecerá en la interfaz web de Spark (localhost:8080).
# El 'master' apunta específicamente al nodo central de nuestro clúster Docker.
spark = SparkSession.builder \
    .appName("Resolucion_TP5_Ecommerce") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Reducimos la cantidad de mensajes por consola para que los resultados sean legibles.
spark.sparkContext.setLogLevel("ERROR")

# Cargamos el archivo CSV desde la ruta montada en el volumen de Docker.
# header=True indica que la primera fila tiene los nombres de las columnas.
# inferSchema=True le pide a Spark que intente adivinar si una columna es texto, número, etc.
df_pedidos = spark.read.csv("/opt/spark/data/e-commerce_orders.csv", header=True, inferSchema=True)

# -----------------------------------------------------------------------------
# 2. EXPLORACIÓN Y LIMPIEZA
# -----------------------------------------------------------------------------

print("--- Exploración inicial del Dataset ---")
# La acción 'show()' fuerza a Spark a procesar los datos y mostrar las primeras 5 filas por consola.
df_pedidos.show(5)

# Limpieza: eliminamos cualquier registro (fila) que contenga valores nulos en alguna columna.
df_limpio = df_pedidos.dropna()

# Limpieza: aseguramos que la columna 'Price' sea tratada matemáticamente como un número decimal (float).
# withColumn sobrescribe la columna existente aplicando la transformación (cast).
df_limpio = df_limpio.withColumn("Price", col("Price").cast("float"))

print("--- Esquema de datos tras la limpieza ---")
# Imprimimos la estructura para verificar que 'Price' ahora sea float y no string.
df_limpio.printSchema()

# -----------------------------------------------------------------------------
# 3. TRANSFORMACIONES Y MÉTRICAS
# -----------------------------------------------------------------------------

# a. Calcular el monto total y promedio de ventas.
# Usamos la función agg() para aplicar múltiples operaciones de agregación al mismo tiempo.
# alias() simplemente renombra la columna resultante para que quede más prolija en la tabla.
print("--- a. Monto Total y Promedio de Ventas ---")
metricas_ventas = df_limpio.agg(
    sum("Price").alias("Monto_Total_Ventas"),
    avg("Price").alias("Promedio_Ventas")
)
metricas_ventas.show()

# b. Contar la cantidad de pedidos por estado (Ejecutado, Pendiente o Suspendido).
# groupBy agrupa las filas que tienen el mismo valor en 'status', y count() las cuenta.
print("--- b. Cantidad de pedidos por estado ---")
pedidos_por_estado = df_limpio.groupBy("status").count().alias("Cantidad")
pedidos_por_estado.show()

# c. Obtener el top 3 de clientes que más dinero gastaron.
# 1. Agrupamos por el ID del cliente.
# 2. Sumamos todo el dinero que gastó cada uno.
# 3. Ordenamos de forma descendente (desc) para que el mayor quede arriba.
# 4. Limitamos la salida a los primeros 3 resultados.
print("--- c. Top 3 de clientes con mayor gasto ---")
top_clientes = df_limpio.groupBy("customer_id") \
    .agg(sum("Price").alias("Total_Gastado")) \
    .orderBy(col("Total_Gastado").desc()) \
    .limit(3)
top_clientes.show()

# -----------------------------------------------------------------------------
# 4. FINALIZAR SESIÓN
# -----------------------------------------------------------------------------
# Es una buena práctica detener la sesión al terminar para liberar los recursos del clúster.
spark.stop()