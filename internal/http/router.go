package http

import (
	"encoding/json"
	stdhttp "net/http"
)

func NewRouter() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	return mux
}

func healthHandler(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	if r.Method != stdhttp.MethodGet {
		w.Header().Set("Allow", stdhttp.MethodGet)
		stdhttp.Error(w, "method not allowed", stdhttp.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(stdhttp.StatusOK)

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
