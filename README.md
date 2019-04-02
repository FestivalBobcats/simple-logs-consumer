Start server
```shell
go run main.go
```

Post logs
```shell
curl -XPOST localhost:8080/logs -d '[{"log": "message"}]'
```
