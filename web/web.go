package web

import (
	"distributed-key-value-store/config"
	"distributed-key-value-store/db"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db      *db.Database
	shards  *config.Shards
	replica bool
}

// NewServer creates a new Server instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database, shards *config.Shards, replica bool) *Server {
	return &Server{
		db:      db,
		shards:  shards,
		replica: replica,
	}
}

// Redirects the request to the respective shard
func (s *Server) Redirect(shard int, w http.ResponseWriter, r *http.Request) {
	resp, err := http.Get("http://" + s.shards.Addrs[shard] + r.RequestURI)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting the request: %v", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(w, resp.Body)
}

// GetHandler handles read requests for the database.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		log.Fatal(err)
	}
	key := r.Form.Get("key")
	shard := s.shards.Index([]byte(key))
	if shard != s.shards.CurIdx {
		slog.Info("Incorrect shard, redirecting to correct shard", "current_shard", s.shards.CurIdx, "redirected_shard", shard)
		s.Redirect(shard, w, r)
		return
	}
	value, err := s.db.GetKey([]byte(key))
	fmt.Fprintf(w, "Called get for 'key' %s, get 'value' %q, err %v", key, value, err)
}

// SetHandler handles write requests for the database.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	shard := s.shards.Index([]byte(key))
	if shard != s.shards.CurIdx {
		slog.Info("Incorrect shard, redirecting to correct shard", "current_shard", s.shards.CurIdx, "redirected_shard", shard)
		s.Redirect(shard, w, r)
		return
	}
	value := r.Form.Get("value")
	err := s.db.SetKey([]byte(key), []byte(value))
	fmt.Fprintf(w, "Called set for 'key' %s, 'value' %q, err %v", key, value, err)
}

// DeleteExtraKeys deletes keys that don't belong to the current shard.
func (s *Server) DeleteExtraKeys(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key []byte) bool {
		shard := s.shards.Index([]byte(key))
		return shard != s.shards.CurIdx
	}))
}

// Listen the server at specified address
func (s *Server) ListenAndServe(addr string) {
	fmt.Printf("Server is running on %s...\n", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
