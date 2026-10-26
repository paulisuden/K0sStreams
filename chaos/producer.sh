#!/bin/sh
# Productor de la suite de caos. Durante los segundos pedidos publica <tópico>-1, <tópico>-2, ... y por cada mensaje
# imprime una línea:
#   CONFIRMED <valor>            el broker respondió 200: el mensaje tiene que estar en el log pase lo que pase
#   UNCONFIRMED <valor> <http>   cualquier otra respuesta (000 = ninguna): puede estar o no, las dos cosas son válidas
#
# Corre adentro del cluster, con el sh de la imagen curlimages/curl (no tiene bash): un seguidor responde 307 hacia
# broker-N.broker, un nombre que solo resuelve el DNS del cluster. curl -L sigue el 307 y repite el POST con el mismo cuerpo.
#
# Va por tiempo y no por cantidad: a unos 100 mensajes por segundo, 300 mensajes terminaban antes de que llegara la falla.
#
# Uso: producer.sh <tópico> <segundos>        BROKER opcional, por defecto http://broker-lb:9090

set -u

topic="$1"
seconds="$2"
broker="${BROKER:-http://broker-lb:9090}"

# Imprime solo el código HTTP final. El máximo de 15 s cubre un failover sin aviso (el Lease vence a los 10 s).
post() {
  curl -s -L -o /dev/null -w '%{http_code}' --max-time 15 -X POST "$broker$1" \
    -H 'Content-Type: application/json' -d "$2"
}

# 409 también sirve: puede ser el reintento de un 201 cuya respuesta se perdió.
while :; do
  case "$(post /v1/topics "{\"name\":\"$topic\"}")" in
    201 | 409) break ;;
  esac
  sleep 1
done

end=$(($(date +%s) + seconds))
i=1
while [ "$(date +%s)" -lt "$end" ]; do
  value="$topic-$i"
  status="$(post "/v1/topics/$topic/messages" "{\"value\":\"$value\"}")"
  if [ "$status" = 200 ]; then
    echo "CONFIRMED $value"
  else
    echo "UNCONFIRMED $value $status"
    # Sin líder, el 503 vuelve al instante: sin esta pausa un failover se gasta cientos de mensajes en un segundo.
    sleep 1
  fi
  i=$((i + 1))
done

echo "DONE"
