#!/bin/sh
# Lector de la suite de caos. Por cada broker imprime una línea "<broker> <valor>" por mensaje confirmado (hasta su high
# watermark), o "<broker> ERROR" si no lo pudo leer entero.
#
# Corre adentro del cluster para leer cada réplica por su nombre (broker-N.broker): a través del LoadBalancer no se sabe
# qué broker responde. La imagen curlimages/curl no trae jq; el JSON se recorta con grep y sed, y alcanza porque los
# valores que publica producer.sh son <tópico>-<n>, sin comillas ni comas.
#
# Uso: read-topic.sh <tópico> <broker>...        por ejemplo: read-topic.sh chaos-1 broker-0.broker broker-1.broker

set -u

topic="$1"
shift

field() {
  sed -n "s/.*\"$1\":\(-\{0,1\}[0-9]*\).*/\1/p"
}

for broker in "$@"; do
  from=0
  while :; do
    if ! page="$(curl -s -f --max-time 5 "http://$broker:9090/v1/topics/$topic/partitions/0/messages?from=$from&max=500")"; then
      echo "$broker ERROR"
      break
    fi

    echo "$page" | grep -o '"value":"[^"]*"' | cut -d '"' -f 4 | sed "s/^/$broker /"

    high_watermark="$(echo "$page" | field highWatermark)"
    next_offset="$(echo "$page" | field nextOffset)"
    # Pasado el high watermark no hay nada confirmado. Sin avance, se terminó el log.
    if [ "$next_offset" -gt "$high_watermark" ] || [ "$next_offset" -le "$from" ]; then
      break
    fi
    from="$next_offset"
  done
done
