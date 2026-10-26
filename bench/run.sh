#!/usr/bin/env bash
# Benchmark simple (fase 5): corre bench.js con k6 adentro del cluster, para cada modo y cada cantidad de clientes, y
# arma una tabla. Los tres modos usan el mismo tópico nuevo, en orden: lo que publica "publish" lo leen "read" y "queue".
#
# Por defecto le pega directo al líder, para medir el broker y no el redirect. Por el LoadBalancer (2 de cada 3
# escrituras pagan un 307): BROKER=http://broker-lb:9090 bench/run.sh
#
# Uso: bench/run.sh [segundos por corrida, por defecto 20] [clientes..., por defecto 1 10 50]

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K6_IMAGE="grafana/k6:0.54.0"

seconds="${1:-20}"
shift || true
clients=("$@")
if [ ${#clients[@]} -eq 0 ]; then
  clients=(1 10 50)
fi

leader="$(kubectl get lease k0sstreams-leader -o jsonpath='{.spec.holderIdentity}')"
broker="${BROKER:-http://$leader.broker:9090}"
topic="bench-$(date +%m%d-%H%M%S)"
out="$BENCH_DIR/results/$(date +%m%d-%H%M)"
mkdir -p "$out"

# Una corrida de k6 en un pod. El script viaja como argumento de "sh -c", igual que en chaos/lib.sh: no hace falta
# copiarlo ni crear un ConfigMap.
run_k6() {
  local mode="$1" vus="$2" name="bench-$1-$2"
  kubectl run "$name" --image="$K6_IMAGE" --restart=Never \
    --env="BROKER=$broker" --env="TOPIC=$topic" --env="MODE=$mode" --env="VUS=$vus" --env="DURATION=${seconds}s" \
    --command -- sh -c 'printf "%s" "$1" > /tmp/bench.js && exec k6 run --quiet --no-color /tmp/bench.js' \
    sh "$(cat "$BENCH_DIR/bench.js")" >/dev/null

  local status=0
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout="$((seconds + 120))s" >/dev/null || status=$?
  kubectl logs "$name" >"$out/$name.log"
  kubectl delete pod "$name" >/dev/null
  if [ "$status" -ne 0 ]; then
    echo "La corrida $name falló; ver $out/$name.log" >&2
    return "$status"
  fi
  grep '^RESULT ' "$out/$name.log" | cut -d' ' -f2-
}

echo "Líder: $leader · broker: $broker · tópico: $topic · ${seconds}s por corrida" | tee "$out/summary.txt"
{
  echo "modo clientes ops/s msgs/s p50_ms p95_ms p99_ms fallas_% vacías"
  for mode in publish read queue; do
    for vus in "${clients[@]}"; do
      run_k6 "$mode" "$vus"
    done
  done
} | column -t | tee -a "$out/summary.txt"
echo "Resultados en $out"
