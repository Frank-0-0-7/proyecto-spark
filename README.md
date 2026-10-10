# proyecto-spark

## Para la consola:

### Para limpiar los contenedores

```bash
docker compose down
```

### 1. Comando para ejecutar contenedores

```bash
docker compose up -d
```

### 2. Comando para ejecutar el PySpark

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/main.py
```

## Para la web:

### Para ver la UI web de Spark

```bash
http://localhost:8080/
```

### Para ver la UI web del job

```bash
http://localhost:4040/
```