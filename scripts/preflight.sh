#!/usr/bin/env bash
set -euo pipefail
kubectl version
kubectl get nodes -o wide
kubectl get storageclass
kubectl get events -n kafka --sort-by=.lastTimestamp 2>/dev/null || true
