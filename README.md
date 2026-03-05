# Pipeline de Análisis de Trazas de Aplicación con Big Data

Este proyecto implementa un pipeline de datos de extremo a extremo para la recolección, procesamiento, almacenamiento y consulta en tiempo real de trazas de solicitudes de una aplicación. La arquitectura está completamente contenerizada utilizando Docker y Docker Compose, demostrando la integración de tecnologías líderes en el ecosistema de Big Data.

## Arquitectura

El flujo de datos a través del sistema es el siguiente:

1.  **Generación de Trazas (`server-mesh`)**: Las aplicaciones (ej. `app1`, `app2`) se ejecutan junto a un sidecar de Envoy. Envoy intercepta todo el tráfico de red, genera logs de acceso y los reenvía.
2.  **Recolección de Logs (`Fluent-Bit`)**: Otro sidecar, Fluent Bit, recolecta los logs de Envoy y los envía a un tópico de Kafka.
3.  **Broker de Mensajería (`Kafka`)**: Actúa como un búfer centralizado y resiliente, recibiendo las trazas en el tópico `envoy-logs`. Desacopla a los productores de los consumidores.
4.  **Procesamiento en Streaming (`Spark`)**: Una aplicación de Spark Structured Streaming se suscribe al tópico de Kafka, procesa los mensajes en micro-lotes y los escribe en formato Parquet en el sistema de archivos distribuido HDFS.
5.  **Almacenamiento Distribuido (`Hadoop HDFS`)**: HDFS proporciona un almacenamiento escalable y tolerante a fallos para los archivos Parquet. El clúster consiste en un NameNode y DataNodes desplegados dinámicamente.
6.  **Consulta Federada (`Trino` + `Hive Metastore`)**:
    *   **Hive Metastore**: Almacena los metadatos (esquema, ubicación de archivos) de las tablas que apuntan a los datos en HDFS.
    *   **Sincronizador de Metadatos**: Un script se ejecuta periódicamente para que Hive reconozca las nuevas particiones de datos escritas por Spark.
    *   **Trino (antes PrestoSQL)**: Un motor de consultas SQL distribuido que se conecta al Hive Metastore para permitir a los usuarios ejecutar consultas interactivas sobre los datos.

## Estructura del Proyecto

El repositorio está organizado en los siguientes directorios principales:

*   `zona-deploy/`: Contiene la lógica para desplegar "zonas" completas. Cada subdirectorio representa un componente de la infraestructura.
    *   `server-mesh/`: Define el patrón de sidecar con Envoy y Fluent-Bit para las aplicaciones.
    *   `streaming-kafka/`: Despliega el bróker de Kafka.
    *   `spark-streaming-app/`: Despliega la aplicación de procesamiento con Spark.
    *   `datanode/`: Plantilla para desplegar datanodes de HDFS.
    *   `deploy-zone.sh` y `destroy-zone.sh`: Scripts para crear y destruir una zona completa.
*   `hadoop-cluster/`: Define el servicio HDFS NameNode.
*   `trino-sql/`: Despliega el stack de consulta con Trino, Hive Metastore y el sincronizador de particiones.
*   `sql/`: Contiene scripts SQL, como la creación de la tabla inicial en Hive.

## Prerrequisitos

*   Docker y Docker Compose
*   Asegúrate de que Docker tenga asignados suficientes recursos (se recomiendan +8GB de RAM).

## Cómo Empezar

El despliegue está automatizado a través de scripts que gestionan "zonas". Una zona es un entorno de despliegue autocontenido.

### Paso 1: Crear la Red Principal

Antes de desplegar cualquier componente, es necesario crear la red principal que permitirá la comunicación entre los distintos contenedores de la zona.

```bash
docker network create data-backbone
```

### Paso 2: Desplegar una Zona

Para desplegar una zona completa (por ejemplo, `zone_1`) con `app1`, ejecuta el siguiente script. Este se encargará de levantar todos los servicios en el orden correcto.

```bash
cd zona-deploy/
./deploy-zone.sh zone_1 app1 8081
```

Este comando despliega `app1` en una nueva zona llamada `zone_1`, exponiendo el servicio en el puerto `8081` del host.

### Paso 3: Desplegar el NameNode de HDFS

El NameNode de HDFS se gestiona de forma separada.

```bash
cd ../hadoop-cluster/
docker-compose up -d
```

### Paso 4: Desplegar el Stack de Trino/Hive

Finalmente, levanta la capa de consulta.

```bash
cd ../trino-sql/
docker-compose up -d
```

## Uso del Pipeline

### 1. Generar Datos de Trazas

Envía solicitudes a la aplicación desplegada. Usando el ejemplo anterior, `app1` está en el puerto `8081`.

```bash
# Realiza una petición GET a la API de productos
curl http://localhost:8081/app_1/products

# Crea un nuevo producto
curl -X POST http://localhost:8081/app_1/products \
  -H "Content-Type: application/json" \
  -d '{"name": "My New Product", "price": 19.99, "category": "Books"}'
```

Cada solicitud generará logs que son capturados y enviados a Kafka.

### 2. Consultar los Datos con Trino

Después de unos minutos, la aplicación Spark habrá procesado los datos. Puedes usar Trino para consultarlos.

1.  Conéctate a Trino a través de su CLI o una herramienta de BI (DBeaver, Superset) en el puerto `8080`.
2.  Ejecuta consultas SQL sobre la tabla.

```sql
-- Verificar los últimos registros en la tabla de trazas
SELECT *
FROM hive.default.trazas_logs_v3
LIMIT 10;

-- Contar el número de peticiones por método HTTP
SELECT request_method, COUNT(*) as total
FROM hive.default.trazas_logs_v3
GROUP BY request_method
ORDER BY total DESC;
```

## Detener el Entorno

Para detener y limpiar todos los recursos, utiliza los scripts correspondientes.

### Paso 1: Destruir la Zona

Usa el script `destroy-zone.sh` para detener y eliminar todos los contenedores asociados a una zona.

```bash
cd zona-deploy/
./destroy-zone.sh zone_1
```

### Paso 2: Detener los Servicios Centrales

Detén el NameNode y el stack de Trino/Hive.

```bash
cd ../hadoop-cluster/
docker compose down

cd ../trino-sql/
docker compose down
```

### Paso 3: Eliminar la Red

Finalmente, puedes eliminar la red principal si ya no la necesitas.

```bash
docker network rm data-backbone
```
