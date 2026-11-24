package raft

import (
	"log/slog"
	"os"
)

var logLevel = new(slog.LevelVar)

func initLogger() {
	os.MkdirAll("/tmp/raft-logs", 0o755)
	f, err := os.OpenFile("/tmp/raft-logs/raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})))
}
