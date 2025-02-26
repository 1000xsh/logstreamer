# logstreamer
stream your log file in real time. its still in beta

# setup
```
go mod init logstreamer
go mod tidy
go build -o logstreamer main.go
```

# features
- real time monitoring (handling file rotations)
- regex-based filtering
- prometheus integration
- cpu affinity
