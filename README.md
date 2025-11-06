```bash
func azure functionapp publish appFunctionPyApp2 --python --force
```

Checking Identity
```bash
az functionapp log tail --name appFunctionPyApp2 --resource-group testResourcev2
az functionapp identity assign --name appFunctionPyApp2 --resource-group testResourcev2
```

