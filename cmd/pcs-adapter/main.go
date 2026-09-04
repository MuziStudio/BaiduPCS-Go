// pcs-adapter exposes BaiduPCS-Go's upload API over HTTP with SSE progress streaming.
//
// Endpoints:
//
//	POST /upload
//	  Request:  {"job_id":"...","local_path":"...","remote_path":"..."}
//	  Response: 202 Accepted, {"job_id":"..."}
//
//	GET /upload/{job_id}/stream
//	  Response: text/event-stream
//	  event: progress  data: {"uploaded":N,"total":N,"speed":N}
//	  event: done      data: {}
//	  event: error     data: {"message":"..."}
//
// Run `BaiduPCS-Go login` once on the server before starting this process.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/qjfoidnh/BaiduPCS-Go/internal/pcscommand"
	"github.com/qjfoidnh/BaiduPCS-Go/internal/pcsconfig"
)

var configPath = flag.String("config", "", "path to panpipe JSON config")

type adapterConfig struct {
	AdapterAddr string `json:"adapter_addr"`
	BaiduBDUSS  string `json:"baidu_bduss"`
	BaiduSTOKEN string `json:"baidu_stoken"`
}

func main() {
	flag.Parse()
	if *configPath == "" {
		log.Fatal("config file is required")
	}
	cfg, err := loadAdapterConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	pcsconfig.Config.Init()
	if cfg.BaiduBDUSS == "" || cfg.BaiduSTOKEN == "" {
		log.Fatal("baidu_bduss and baidu_stoken are required")
	}
	if _, err := pcsconfig.Config.SetupUserByBDUSS(cfg.BaiduBDUSS, "", cfg.BaiduSTOKEN, ""); err != nil {
		log.Fatalf("baidu login: %v", err)
	}

	srv := newServer()

	hs := &http.Server{Addr: cfg.AdapterAddr, Handler: srv.mux}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(ctx)
	}()

	log.Printf("pcs-adapter listening on %s", cfg.AdapterAddr)
	if err := hs.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

type panpipeConfig struct {
	BaiduBDUSS  string `json:"baidu_bduss"`
	BaiduSTOKEN string `json:"baidu_stoken"`
}

func loadAdapterConfig(path string) (adapterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return adapterConfig{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg adapterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return adapterConfig{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	return cfg, nil
}

// progressEvent is sent as an SSE data payload.
type progressEvent struct {
	Uploaded int64  `json:"uploaded,omitempty"`
	Total    int64  `json:"total,omitempty"`
	Speed    int64  `json:"speed,omitempty"`
	Message  string `json:"message,omitempty"`
}

// jobStream holds the SSE channel for one in-flight upload job.
type jobStream struct {
	ch     chan progressEvent
	evType chan string // parallel channel carrying the SSE event name
	done   chan struct{}
}

type server struct {
	mux     *http.ServeMux
	mu      sync.Mutex
	streams map[string]*jobStream
}

func newServer() *server {
	s := &server{
		mux:     http.NewServeMux(),
		streams: make(map[string]*jobStream),
	}
	s.mux.HandleFunc("POST /upload", s.handleUpload)
	s.mux.HandleFunc("GET /upload/{job_id}/stream", s.handleStream)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return s
}

type uploadRequest struct {
	JobID      string `json:"job_id"`
	LocalPath  string `json:"local_path"`
	RemotePath string `json:"remote_path"`
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	var req uploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.JobID == "" || req.LocalPath == "" || req.RemotePath == "" {
		http.Error(w, "job_id, local_path and remote_path are required", http.StatusBadRequest)
		return
	}

	stream := &jobStream{
		ch:     make(chan progressEvent, 16),
		evType: make(chan string, 16),
		done:   make(chan struct{}),
	}
	s.mu.Lock()
	s.streams[req.JobID] = stream
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.streams, req.JobID)
			s.mu.Unlock()
			close(stream.done)
		}()

		err := pcscommand.RunUpload([]string{req.LocalPath}, req.RemotePath, &pcscommand.UploadOptions{
			ProgressHook: func(uploaded, total, speed int64) {
				stream.evType <- "progress"
				stream.ch <- progressEvent{Uploaded: uploaded, Total: total, Speed: speed}
			},
		})
		if err != nil {
			stream.evType <- "error"
			stream.ch <- progressEvent{Message: err.Error()}
			return
		}
		stream.evType <- "done"
		stream.ch <- progressEvent{}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": req.JobID})
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")

	// Wait briefly for the job to be registered (race between POST returning and GET arriving).
	var stream *jobStream
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		stream = s.streams[jobID]
		s.mu.Unlock()
		if stream != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if stream == nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case evType := <-stream.evType:
			ev := <-stream.ch
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, data)
			flusher.Flush()
			if evType == "done" || evType == "error" {
				return
			}
		case <-stream.done:
			return
		}
	}
}
