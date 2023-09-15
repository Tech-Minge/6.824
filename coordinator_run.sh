
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
echo "build wc.so"
go run -race mrcoordinator.go pg-*.txt