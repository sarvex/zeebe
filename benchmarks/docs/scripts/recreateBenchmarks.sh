#!/bin/bash
set -xou pipefail

for ns in $(kubectl get namespaces | grep $1 | awk '{print $1 }'); 
do
  echo $ns;
  values=$(helm get values "$ns" --namespace "$ns" -o yaml);
  echo "$values" > "$ns-values.yaml"
  helm uninstall "$ns" --namespace "$ns"
  kubectl delete pvc -l app=elasticsearch-master --namespace="$ns"
  kubectl delete pvc -l app=camunda-platform --namespace="$ns"
  echo "$values" | helm install "$ns" zeebe-benchmark/zeebe-benchmark --namespace "$ns" --values -; 
done
