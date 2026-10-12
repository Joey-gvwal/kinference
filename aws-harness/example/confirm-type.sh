#!/bin/sh

kubectl get nodes -l role=gpu -o custom-columns='NAME:.metadata.name,INSTANCE:.metadata.labels.node\.kubernetes\.io/instance-type'
