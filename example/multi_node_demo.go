//go:build ignore

// Run three processes against the same etcd:
//
//	go run example/multi_node_demo.go -port 8001 -api 9001
//	go run example/multi_node_demo.go -port 8002 -api 9002
//	go run example/multi_node_demo.go -port 8003 -api 9003
//
// GET demonstrates owner routing + optional near cache.
// PUT/DELETE demonstrate explicit cache mutation routed to the owner.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	locache "github.com/scut2024hjt/locache"
)

var (
	sourceMu sync.RWMutex
	source   = map[string]string{
		"Tom":   "630",
		"Jack":  "589",
		"Sam":   "567",
		"Alice": "710",
		"Bob":   "420",
	}
)

func loadSource(_ context.Context, key string) ([]byte, error) {
	sourceMu.RLock()
	value, ok := source[key]
	sourceMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}
	return []byte(value), nil
}

func main() {
	port := flag.Int("port", 8001, "gRPC peer port")
	apiPort := flag.Int("api", 9001, "HTTP demo port")
	flag.Parse()

	listenAddr := fmt.Sprintf("localhost:%d", *port)
	endpoints := []string{"localhost:2379"}
	server, err := locache.NewServer(listenAddr, "locache", locache.WithEtcdEndpoints(endpoints))
	if err != nil {
		log.Fatal(err)
	}
	defer server.Stop()

	picker, err := locache.NewClientPicker(
		server.Address(),
		locache.WithServiceName("locache"),
		locache.WithPickerEtcdEndpoints(endpoints),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer picker.Close()

	group := locache.NewGroup(
		"scores",
		2<<20,
		locache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[source] owner %s loads %q", server.Address(), key)
			time.Sleep(200 * time.Millisecond)
			return loadSource(ctx, key)
		}),
		locache.WithPeers(picker),
		locache.WithExpiration(30*time.Second),
		locache.WithNearCache(256<<10, 3*time.Second),
	)
	defer group.Close()

	go func() {
		if err := server.Start(); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	http.HandleFunc("/api/cache", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			view, err := group.Get(r.Context(), key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			fmt.Fprintf(w, "node=%s key=%s value=%s\n", server.Address(), key, view.String())

		case http.MethodPut:
			value, err := io.ReadAll(r.Body)
			if err != nil || len(value) == 0 {
				http.Error(w, "request body must contain value", http.StatusBadRequest)
				return
			}
			if err := group.Set(r.Context(), key, value); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			fmt.Fprintf(w, "cached key=%s value=%s\n", key, string(value))

		case http.MethodDelete:
			if err := group.Delete(r.Context(), key); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			fmt.Fprintf(w, "evicted key=%s\n", key)

		default:
			w.Header().Set("Allow", "GET, PUT, DELETE")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	apiAddr := fmt.Sprintf("localhost:%d", *apiPort)
	log.Printf("node=%s members=%v", server.Address(), picker.Members())
	log.Printf("GET    http://%s/api/cache?key=Tom", apiAddr)
	log.Printf("PUT    curl -X PUT --data '999' 'http://%s/api/cache?key=Tom'", apiAddr)
	log.Printf("DELETE curl -X DELETE 'http://%s/api/cache?key=Tom'", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr, nil))
}
