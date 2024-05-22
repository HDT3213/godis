#正式版打包
set GOOS=linux
set GOARCH=amd64
go.exe build -ldflags "-s -w" -o target/godis-linux ./
