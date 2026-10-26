#!/usr/bin/env bash
# Funciones comunes de la suite de caos. Se cargan con "source"; corren en tu máquina y hablan con el cluster por kubectl.

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURL_IMAGE="curlimages/curl:8.10.1"

# Arranca un pod que corre un script de chaos/ con sh. El script viaja como texto en "sh -c": no hace falta copiarlo al pod.
# Uso: start_pod <nombre-del-pod> <script> <argumentos>...
start_pod() {
  local name="$1" script="$2"
  shift 2
  kubectl run "$name" --image="$CURL_IMAGE" --restart=Never --command -- \
    sh -c "$(cat "$CHAOS_DIR/$script")" "$script" "$@" >/dev/null
}

# Espera a que el pod termine, imprime su salida y lo borra (también si no terminó a tiempo, para poder repetir el nombre).
# Uso: pod_output <nombre-del-pod> [segundos de espera, por defecto 120]
pod_output() {
  local name="$1" timeout="${2:-120}" status=0
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout="${timeout}s" >/dev/null || status=$?
  kubectl logs "$name"
  kubectl delete pod "$name" >/dev/null
  return "$status"
}

# Imprime los nombres DNS de los brokers (broker-0.broker ...), según las réplicas del StatefulSet.
broker_hosts() {
  local replicas
  replicas="$(kubectl get statefulset broker -o jsonpath='{.spec.replicas}')"
  for ((i = 0; i < replicas; i++)); do
    echo "broker-$i.broker"
  done
}
