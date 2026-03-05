# Pruebas de Estrés en Trino

Este directorio contiene herramientas para validar el rendimiento y los límites de memoria de Trino dentro del ecosistema Big Data del proyecto.

## Objetivo
El propósito de estas pruebas es estresar el motor de consultas de Trino para verificar su estabilidad y sus mecanismos de protección de recursos (Resource Groups / Memory Limits) sin afectar los datos reales almacenados en el Data Lake.

## Metodología: El Conector TPCH
Para estas pruebas utilizamos el conector **TPCH**. A diferencia de otros conectores, TPCH no lee archivos de disco ni consulta bases de datos externas.

*   **Datos Sintéticos**: Los datos se generan en memoria mediante algoritmos matemáticos al momento de ejecutar la consulta.
*   **Zero Footprint**: No deja rastro de datos en **HDFS** ni ensucia el **Hive Metastore**.
*   **Aislamiento**: Es ideal para medir puramente el rendimiento de CPU y RAM de los nodos de Trino.

## Escenarios de Prueba

1.  **Carga Moderada (SF1)**: Procesa un esquema de aproximadamente 1GB (Escala 1). Útil para validar que la conectividad básica y la ejecución de consultas complejas funcionan.
2.  **Estrés Pesado (SF10)**: Procesa un esquema de aproximadamente 10GB (Escala 10). Esta prueba realiza JOINS masivos entre tablas de millones de registros (`lineitem` ~60M filas).
    *   **Nota**: En configuraciones con recursos limitados, es normal que esta consulta falle con un error de `Query exceeded per-node memory limit`. Esto es un **indicador de éxito** de la prueba, ya que confirma que los límites de seguridad de Trino están activos.

## Cómo ejecutar
Asegúrate de que el contenedor de Trino esté corriendo y ejecuta:

```bash
chmod +x tests/trino-stress-test.sh
./tests/trino-stress-test.sh
```

## Análisis de Resultados
Si la consulta SF10 falla con un límite de memoria, significa que Trino está protegiendo al sistema de un desbordamiento (OOM). Si deseas que la consulta termine, deberás aumentar los límites en la configuración de Trino (`query.max-memory-per-node` en `config.properties`).
