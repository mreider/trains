#!/bin/bash

# Deploy all Kubernetes configurations in the k8s directory into the 'trains-demo' namespace.
kubectl create namespace trains-demo
kubectl label namespace trains-demo dynatrace.com/inject=true --overwrite
kubectl apply -f k8s/ --namespace=trains-demo --validate=false

echo "Deployments applied to namespace 'trains-demo'."

# Restart all deployments and statefulsets to force pulling the latest images
for d in $(kubectl get deployments -n trains-demo -o name); do
  kubectl rollout restart $d -n trains-demo
done
for s in $(kubectl get statefulsets -n trains-demo -o name); do
  kubectl rollout restart $s -n trains-demo
done

echo "All workloads restarted to pull the latest images."
