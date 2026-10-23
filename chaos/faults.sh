#!/usr/bin/env bash
# Fallas de la suite de caos. Se carga con "source" y cada función fault_* rompe el cluster de una manera e imprime qué
# hizo. No esperan a que el cluster se recupere: para eso está wait_healthy.
#
# Con aviso = kubectl delete normal: el broker recibe SIGTERM, suelta el Lease y otro lo toma en un par de segundos.
# Sin aviso = --grace-period=0 --force: el proceso muere en el acto, como un corte de luz. Nadie suelta el Lease y hay
# que esperar a que venza (10 s), salvo que el StatefulSet recree antes el mismo pod y lo retome con la época siguiente.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

LEASE_NAME="k0sstreams-leader"

leader() {
  kubectl get lease "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}'
}

epoch() {
  kubectl get lease "$LEASE_NAME" -o jsonpath='{.spec.leaseTransitions}'
}

# Un broker al azar que no sea el líder.
follower() {
  local current_leader
  current_leader="$(leader)"
  kubectl get pods -l app=broker -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
    | grep -vx "$current_leader" | shuf -n 1
}

kill_pod() {
  kubectl delete pod "$1" --wait=false >/dev/null
}

# kubectl avisa por stderr que el borrado forzado no espera confirmación: es justamente lo que queremos.
kill_pod_force() {
  kubectl delete pod "$1" --grace-period=0 --force 2>/dev/null >/dev/null
}

# Cantidad de brokers Ready que no se están apagando.
ready_brokers() {
  kubectl get pods -l app=broker \
    -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}|{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    | grep -cx '|True'
}

# Espera a que todos los brokers estén Ready y el Lease tenga un titular entre ellos. Sale con 1 si no pasa a tiempo.
# Uso: wait_healthy [segundos, por defecto 180]
wait_healthy() {
  local timeout="${1:-180}" replicas holder
  replicas="$(kubectl get statefulset broker -o jsonpath='{.spec.replicas}')"
  for ((elapsed = 0; elapsed < timeout; elapsed++)); do
    holder="$(leader)"
    if [[ "$(ready_brokers)" == "$replicas" && -n "$holder" ]] \
      && [[ "$(kubectl get pod "$holder" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" == True ]]; then
      return 0
    fi
    sleep 1
  done
  echo "el cluster no se recuperó en ${timeout}s" >&2
  return 1
}

fault_leader() {
  local pod
  pod="$(leader)"
  kill_pod "$pod"
  echo "maté al líder $pod con aviso (época $(epoch))"
}

fault_leader_force() {
  local pod
  pod="$(leader)"
  kill_pod_force "$pod"
  echo "maté al líder $pod sin aviso (época $(epoch))"
}

fault_follower() {
  local pod
  pod="$(follower)"
  kill_pod_force "$pod"
  echo "maté al seguidor $pod sin aviso"
}

# Líder y un seguidor a la vez: queda un solo broker y sin quórum no se puede confirmar nada hasta que vuelvan.
fault_no_quorum() {
  local leader_pod follower_pod
  leader_pod="$(leader)"
  follower_pod="$(follower)"
  kill_pod_force "$leader_pod"
  kill_pod_force "$follower_pod"
  echo "maté al líder $leader_pod y al seguidor $follower_pod sin aviso: sin quórum"
}

# Tres cambios de líder seguidos, cada uno apenas el anterior se recuperó: prueba las épocas y el fencing en cadena.
fault_leader_repeated() {
  for _ in 1 2 3; do
    fault_leader_force
    wait_healthy || return 1
  done
}
