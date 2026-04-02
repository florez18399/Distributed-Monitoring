#!/bin/sh

echo "Iniciando servicio de sincronización Hive (Protocol Aware)..."

# Esperar a que Trino esté listo
sleep 15

while true; do
    echo "$(date): Iniciando sincronización..."
    
    # 1. Enviar la consulta inicial
    RESPONSE=$(curl -s -X POST \
      http://trino:8080/v1/statement \
      -H "Content-Type: text/plain" \
      -H "X-Trino-User: hive" \
      -d "CALL hive.system.sync_partition_metadata('default', 'trazas_logs_v5', 'ADD')")

    # 2. Extraer el ID de la consulta para seguimiento
    QUERY_ID=$(echo "$RESPONSE" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
    NEXT_URI=$(echo "$RESPONSE" | grep -o '"nextUri":"[^"]*' | cut -d'"' -f4)

    if [ -z "$QUERY_ID" ]; then
        echo "$(date): ERR - No se pudo iniciar la consulta. Respuesta: $RESPONSE"
    else
        echo "$(date): Consulta iniciada ID: $QUERY_ID"
        
        # 3. Bucle de seguimiento (Poll nextUri)
        # Mientras exista un nextUri, seguimos consultando para mantener la query viva
        while [ ! -z "$NEXT_URI" ]; do
            # echo "Poll: $NEXT_URI"
            RESPONSE=$(curl -s "$NEXT_URI")
            
            # Extraer el siguiente URI (si existe)
            NEXT_URI=$(echo "$RESPONSE" | grep -o '"nextUri":"[^"]*' | cut -d'"' -f4)
            
            # Verificar estado final
            STATE=$(echo "$RESPONSE" | grep -o '"state":"[^"]*' | cut -d'"' -f4)
            
            if [ "$STATE" = "FINISHED" ]; then
                echo "$(date): Sincronización finalizada con éxito."
                break
            fi
            
            if [ "$STATE" = "FAILED" ]; then
                ERROR_MSG=$(echo "$RESPONSE" | grep -o '"message":"[^"]*' | cut -d'"' -f4)
                echo "$(date): ERR - Consulta fallida: $ERROR_MSG"
                break
            fi
            
            # Pequeña espera entre polls para no saturar
            sleep 1
        done
    fi

    echo "$(date): Esperando 60s para la próxima sincronización..."
    sleep 60 
done
