```bash
func azure functionapp publish appFunctionPyApp2 --python --force
```

Checking Identity
```bash
az functionapp log tail --name appFunctionPyApp2 --resource-group testResourcev2
az functionapp identity assign --name appFunctionPyApp2 --resource-group testResourcev2
```

Assigning Identity
```bash
az role assignment create \
  --assignee 0bd482e5-1741-4a0d-aa17-f3aeb12c0a30 \
  --role Reader \
  --scope /subscriptions/1065d3d0-ee68-4e23-9bc7-0211ac514620
```