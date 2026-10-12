# proyecto-spark

## Ejecucion en consola:

### 1. Iniciar el clúster (Master y Workers)

```bash
docker compose up -d
```

### 2. Ejecutar el script PySpark

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/main.py
```

### 3. Detener y limpiar los contenedores

```bash
docker compose down
```

---

## Monitoreo y Web UI:

- **Spark Master UI (Estado del clúster y workers):**

```bash
http://localhost:8080/
```

- **Spark Application / Job UI (DAG, Stages y Tasks):**
#### (Solo aparece durante el proceso del PySpark)

```bash
http://localhost:4040/
```