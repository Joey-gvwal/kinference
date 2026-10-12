#!/bin/sh

for i in `grep -E "^endpoint" ../aws-harness/config.*toml  | awk -F'"' '{print $2}'`
do
  echo $i
  curl -X POST "http://$i/reset_prefix_cache"
  curl -X POST "http://$i/reset_mm_cache"
  curl -X POST "http://$i/reset_encoder_cache"
done
