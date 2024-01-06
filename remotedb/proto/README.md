To compile, use
```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --go-grpc_out=require_unimplemented_servers=false:. \
    defs.proto
```