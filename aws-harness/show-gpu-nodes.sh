#!/bin/sh

kubectl get nodes -l role=gpu \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.conditions[-1].type,INSTANCE:.metadata.labels.node\.kubernetes\.io/instance-type,GPU:.status.capacity.nvidia\.com/gpu'
