#!/usr/bin/env bash
# Suite de caos. Cada ejecución: espera un cluster sano, arranca producer.sh, a los pocos segundos inyecta una falla de
# faults.sh, espera la recuperación, junta los recibos y los pasa por verify.sh. Al final imprime una tabla por escenario.
#
# Uso: run.sh [ejecuciones por escenario, por defecto 5] [escenario...]
#      escenarios: leader leader_force follower no_quorum leader_repeated (por defecto, todos)
# Las salidas quedan en chaos/results/<fecha>/: <tópico>.producer (recibos), <tópico>.verify y summary.txt.

set -euo pipefail
source "$(dirname "$0")/faults.sh"

readonly PRODUCE_SECONDS=30
readonly FAULT_AFTER_SECONDS=5
readonly ALL_SCENARIOS=(leader leader_force follower no_quorum leader_repeated)

runs="${1:-5}"
shift || true
scenarios=("${@:-${ALL_SCENARIOS[@]}}")

stamp="$(date +%m%d-%H%M)"
results="$CHAOS_DIR/results/$stamp"
mkdir -p "$results"

# Si la suite se corta (Ctrl+C, un error), que no queden pods que choquen con la próxima corrida.
cleanup() {
  kubectl delete pod chaos-producer chaos-reader --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Adentro de un "if", bash ignora set -e: por eso cada paso corta con "|| return 1".
run_once() {
  local scenario="$1" topic="$2"
  wait_healthy || return 1
  start_pod chaos-producer producer.sh "$topic" "$PRODUCE_SECONDS" || return 1
  sleep "$FAULT_AFTER_SECONDS"
  "fault_$scenario" || return 1
  wait_healthy || return 1
  pod_output chaos-producer $((PRODUCE_SECONDS + 60)) >"$results/$topic.producer" || return 1
  "$CHAOS_DIR/verify.sh" "$topic" "$results/$topic.producer" | tee "$results/$topic.verify"
}

declare -A passed confirmed unconfirmed
total_runs=0
total_passed=0

for scenario in "${scenarios[@]}"; do
  passed[$scenario]=0
  confirmed[$scenario]=0
  unconfirmed[$scenario]=0
  for ((i = 1; i <= runs; i++)); do
    topic="chaos-$stamp-${scenario//_/-}-$i"
    echo "== $scenario $i/$runs ($topic)"
    if run_once "$scenario" "$topic"; then
      passed[$scenario]=$((passed[$scenario] + 1))
      total_passed=$((total_passed + 1))
    fi
    total_runs=$((total_runs + 1))
    if [[ -f "$results/$topic.producer" ]]; then
      confirmed[$scenario]=$((confirmed[$scenario] + $(grep -c '^CONFIRMED ' "$results/$topic.producer" || true)))
      unconfirmed[$scenario]=$((unconfirmed[$scenario] + $(grep -c '^UNCONFIRMED ' "$results/$topic.producer" || true)))
    fi
  done
done

{
  echo
  printf '%-16s %8s %6s %12s %14s\n' escenario corridas OK confirmados "sin confirmar"
  for scenario in "${scenarios[@]}"; do
    printf '%-16s %8s %6s %12s %14s\n' "$scenario" "$runs" "${passed[$scenario]}" "${confirmed[$scenario]}" "${unconfirmed[$scenario]}"
  done
  echo
  if ((total_passed == total_runs)); then
    echo "0 mensajes confirmados perdidos en $total_runs ejecuciones."
  else
    echo "$((total_runs - total_passed)) de $total_runs ejecuciones fallaron: ver los .verify en $results"
  fi
} | tee "$results/summary.txt"

((total_passed == total_runs))
