export DATABRICKS_SUBDOMAIN=dbc-8f15bc20-a291
export DATABRICKS_CLIENT_ID=7bf9a814-a6ef-405d-ac2c-7bc3c5da79bd
export DATABRICKS_SECRET=dose10036d1e1d4d64c9070f763ebba07208
envsubst < gateway-backend.yaml | kubectl apply -f -
kubectl apply -f provider.yaml
kubectl create secret generic databricks-oauth --from-literal=client-secret=${DATABRICKS_SECRET}
