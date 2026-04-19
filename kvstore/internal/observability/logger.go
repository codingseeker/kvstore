package observability

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

type Logger struct {
	mu  sync.Mutex
	out *log.Logger
}

func NewLogger() *Logger {
	return &Logger{out: log.New(os.Stdout, "", 0)}
}

func (l *Logger) Info(event string, fields map[string]any) {
	l.write("info", event, fields)
}

func (l *Logger) Error(event string, err error, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	l.write("error", event, fields)
}

func (l *Logger) write(level string, event string, fields map[string]any) {
	if l == nil {
		return
	}
	record := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"event": event,
	}
	for key, value := range fields {
		record[key] = value
	}
	b, err := json.Marshal(record)
	if err != nil {
		return
	}
	l.mu.Lock()
	l.out.Print(string(b))
	l.mu.Unlock()
}
