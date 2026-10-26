#!/usr/bin/env bash
# Verificador de la suite de caos. Compara lo que anotó producer.sh con lo que tiene cada réplica del tópico.
#
# Falla (sale con 1) si en alguna réplica:
#   - falta un mensaje CONFIRMED        es una pérdida: lo que la suite tiene que mostrar en cero
#   - un valor aparece dos veces        producer.sh nunca reintenta, así que lo duplicó el broker
#   - hay un valor que nunca se mandó   mensaje inventado o corrupto
#   - el contenido difiere de broker-0  las réplicas no quedaron iguales
# Los UNCONFIRMED pueden estar o no: solo se cuentan.
#
# Un seguidor se entera del high watermark en el siguiente Append o heartbeat (cada 500 ms), y uno que recién volvió
# todavía se está poniendo al día. Por eso, si algo no cierra, vuelve a leer unas veces antes de declarar la falla.
#
# Uso: verify.sh <tópico> <archivo con la salida de producer.sh>

set -euo pipefail
source "$(dirname "$0")/lib.sh"

readonly ATTEMPTS=10
readonly PAUSE_SECONDS=3

topic="$1"
producer_output="$2"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# grep sale con 1 cuando no encuentra nada; sin "|| true", una corrida sin UNCONFIRMED cortaría el script.
{ grep '^CONFIRMED ' "$producer_output" || true; } | cut -d ' ' -f 2 | sort >"$work/confirmed"
{ grep '^UNCONFIRMED ' "$producer_output" || true; } | cut -d ' ' -f 2 | sort >"$work/unconfirmed"
sort "$work/confirmed" "$work/unconfirmed" >"$work/sent"

# Corre el comando y, si imprime algo, lo anota como problema con algunos valores de ejemplo.
report() {
  local host="$1" what="$2" values
  shift 2
  values="$("$@")"
  if [[ -n "$values" ]]; then
    echo "$host: $(wc -l <<<"$values") $what (ej.: $(head -3 <<<"$values" | paste -sd ' '))" >>"$work/problems"
  fi
}

# Lee todas las réplicas y deja en $work/problems lo que no cierra. Vacío = todo bien.
check_replicas() {
  local hosts=("$@") first="$1"
  : >"$work/problems"
  start_pod chaos-reader read-topic.sh "$topic" "${hosts[@]}"
  pod_output chaos-reader >"$work/read"

  for host in "${hosts[@]}"; do
    awk -v host="$host" '$1 == host && $2 != "ERROR" { print $2 }' "$work/read" | sort >"$work/$host"
    if grep -q "^$host ERROR$" "$work/read"; then
      echo "$host: no se pudo leer" >>"$work/problems"
      continue
    fi
    report "$host" "confirmados que faltan" comm -23 "$work/confirmed" "$work/$host"
    report "$host" "duplicados" uniq -d "$work/$host"
    report "$host" "valores que nunca se mandaron" comm -13 "$work/sent" <(sort -u "$work/$host")
    if ! cmp -s "$work/$first" "$work/$host"; then
      echo "$host: distinto de $first" >>"$work/problems"
    fi
  done
}

mapfile -t hosts < <(broker_hosts)
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
  check_replicas "${hosts[@]}"
  if [[ ! -s "$work/problems" ]]; then
    break
  fi
  if ((attempt < ATTEMPTS)); then
    sleep "$PAUSE_SECONDS"
  fi
done

if [[ -s "$work/problems" ]]; then
  echo "FALLA $topic"
  sed 's/^/  /' "$work/problems"
  exit 1
fi

appeared="$(comm -12 "$work/unconfirmed" "$work/${hosts[0]}" | wc -l)"
echo "OK $topic: $(wc -l <"$work/confirmed") confirmados en las ${#hosts[@]} réplicas;" \
  "$(wc -l <"$work/unconfirmed") sin confirmar, de los que aparecieron $appeared"
