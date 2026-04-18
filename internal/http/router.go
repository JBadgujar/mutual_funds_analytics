package http

import (
	"encoding/json"
	stdhttp "net/http"
	"strings"
)

func NewRouter(api *API) stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	if api != nil {
		mux.HandleFunc("/funds", api.HandleListFunds)
		mux.HandleFunc("/funds/rank", api.HandleRankFunds)
		mux.HandleFunc("/sync/trigger", api.HandleSyncTrigger)
		mux.HandleFunc("/sync/status", api.HandleSyncStatus)
		mux.HandleFunc("/funds/", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			path := strings.Trim(r.URL.Path, "/")
			if strings.HasSuffix(path, "/analytics") {
				api.HandleGetFundAnalytics(w, r)
				return
			}
			api.HandleGetFund(w, r)
		})
	}
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
