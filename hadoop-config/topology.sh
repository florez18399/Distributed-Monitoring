#!/bin/bash

# Mapping file co-located with this script in /etc/hadoop/custom/
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAPPING_FILE="$SCRIPT_DIR/topology-mapping.csv"

# Rack por defecto para cualquier host no encontrado (ej. el namenode)
DEFAULT_RACK="/default-rack"

while [ $# -gt 0 ] ; do
  input=$1
  shift

  # If input is an IP address, resolve to hostname via reverse DNS
  if echo "$input" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$'; then
    resolved=$(getent hosts "$input" 2>/dev/null | awk '{print $2}')
    hostname="${resolved:-$input}"
  else
    hostname="$input"
  fi

  # Strip Docker DNS suffix (e.g. zone1-datanode.hdfs-backbone -> zone1-datanode)
  short_name="${hostname%%.*}"

  result=$(grep -v "^#" "$MAPPING_FILE" | grep -v "^$" | grep "^$short_name," | cut -d',' -f2)

  if [ -n "$result" ]; then
    echo "$result"
  else
    echo "$DEFAULT_RACK"
  fi
done
