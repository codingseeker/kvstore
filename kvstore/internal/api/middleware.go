package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"kvstore/internal/observability"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func WithMetrics(next http.Handler, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if metrics != nil {
			metrics.RecordRequest(r.Method, routePath(r.URL.Path), rec.status, time.Since(start))
		}
	})
}

func WithLogging(next http.Handler, logger *observability.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if logger != nil {
			logger.Info("api_request", map[string]any{
				"method":     r.Method,
				"path":       routePath(r.URL.Path),
				"status":     rec.status,
				"latency_ms": time.Since(start).Milliseconds(),
				"request_id": CorrelationID(r),
			})
		}
	})
}

func WithCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			id = generateRequestID()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(withCorrelationID(r.Context(), id)))
	})
}

func WithTokenAuth(next http.Handler, token string) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tokenFromRequest(r) != token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func WithRateLimit(next http.Handler, limit int, metrics *observability.Metrics) http.Handler {
	if limit <= 0 {
		return next
	}
	limiter := newClientLimiter(limit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !limiter.allow(clientID(r)) {
			if metrics != nil {
				metrics.RecordRateLimited()
			}
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func WithHMAC(next http.Handler, secret string) http.Handler {
	if secret == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		signature := strings.TrimSpace(r.Header.Get("X-KV-Signature"))
		if !validSignature(secret, r.Method, r.URL.RequestURI(), body, signature) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid signature"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func WithAudit(next http.Handler, logger *observability.Logger, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if r.Method == http.MethodPut || r.Method == http.MethodDelete {
			if metrics != nil {
				metrics.RecordAuditEvent()
			}
			if logger != nil {
				logger.Info("audit_mutation", map[string]any{
					"method":     r.Method,
					"path":       routePath(r.URL.Path),
					"client":     clientID(r),
					"request_id": CorrelationID(r),
				})
			}
		}
	})
}

func tokenFromRequest(r *http.Request) string {
	if value := r.Header.Get("X-KV-Token"); value != "" {
		return value
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return ""
}

func routePath(path string) string {
	if path == "/status" || path == "/metrics" || path == "/health" {
		return path
	}
	if strings.Trim(path, "/") == "" {
		return "/"
	}
	return "/{key}"
}

func CorrelationID(r *http.Request) string {
	if value, ok := r.Context().Value(correlationIDKey).(string); ok {
		return value
	}
	return strings.TrimSpace(r.Header.Get("X-Request-ID"))
}

func withCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

type clientLimiter struct {
	mu      sync.Mutex
	limit   int
	clients map[string]*clientWindow
}

type clientWindow struct {
	start time.Time
	count int
}

func newClientLimiter(limit int) *clientLimiter {
	return &clientLimiter{limit: limit, clients: map[string]*clientWindow{}}
}

func (l *clientLimiter) allow(client string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	window := l.clients[client]
	if window == nil || now.Sub(window.start) >= time.Second {
		l.clients[client] = &clientWindow{start: now, count: 1}
		return true
	}
	if window.count >= l.limit {
		return false
	}
	window.count++
	return true
}

func clientID(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		if first, _, ok := strings.Cut(forwarded, ","); ok {
			return strings.TrimSpace(first)
		}
		return forwarded
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func validSignature(secret string, method string, uri string, body []byte, signature string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(method))
	mac.Write([]byte("\n"))
	mac.Write([]byte(uri))
	mac.Write([]byte("\n"))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

func generateRequestID() string {
	sum := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(sum[:8])
}
