

curl "http://localhost:6060/debug/pprof/profile?seconds=160" > cpu.pprof

curl http://localhost:6060/debug/pprof/heap > heap.pprof

go tool pprof -http=:8081 cpu.pprof