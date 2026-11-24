package web

import (
	"embed"
	"encoding/json"
	"net/http"
	"time"

	"skiresort/tail-sampling/internal/pipeline"
)

//go:embed index.html
var assets embed.FS

type Server struct {
	pipe *pipeline.Pipeline
	mux  *http.ServeMux
}

func NewServer(pipe *pipeline.Pipeline) *Server {
	s := &Server{pipe: pipe, mux: http.NewServeMux()}
	s.mux.HandleFunc("/api/state", s.handleState)
	s.mux.Handle("/", http.FileServer(http.FS(assets)))
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) ListenAndServe(addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server.ListenAndServe()
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	snap := s.pipe.Stats.Snapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(snap)
}
