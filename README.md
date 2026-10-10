# proyecto-spark

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