# Podstats

Exports resource usage per-container, only using the Pods and Metrics APIs. Current status: working prototype.

```
go build -o podstats cmd/podstats/main.go
./podstats -namespace kube-system -debug
```
