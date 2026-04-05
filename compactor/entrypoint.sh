#!/bin/bash
# Entrypoint: inicia cron en foreground y muestra logs
echo "=== HDFS Compactor Cron Service ==="
echo "Compaction scheduled every 6 hours (last 6h, excluding current)"
echo "Logs: /var/log/compactor.log"
echo "==================================="

# Crear log file
touch /var/log/compactor.log

# Iniciar cron en background
cron

# Seguir los logs en foreground para que Docker no mate el container
tail -f /var/log/compactor.log
