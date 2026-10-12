#!/bin/sh

echo "getting token..."
DATABRICKS_TOKEN=$(curl -s --user '7bf9a814-a6ef-405d-ac2c-7bc3c5da79bd:dose10036d1e1d4d64c9070f763ebba07208' -d 'grant_type=client_credentials&scope=all-apis' https://dbc-8f15bc20-a291.cloud.databricks.com/oidc/v1/token | jq -r .access_token)
echo "got it"

while true; do
  echo pinging...
  curl -H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" -d '{"model":"this is actually ignored","messages":[{"role":"user","content":"say hi"}],"stream":false, "max_tokens":150}' https://dbc-8f15bc20-a291.cloud.databricks.com/serving-endpoints/vllm-endpoint/invocations
  echo
  echo done. sleeping...
  sleep 60
  echo
done
