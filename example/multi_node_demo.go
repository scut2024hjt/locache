package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	lcache "github.com/scut2024hjt/locache"
)

// mockDB 模拟一个慢速后端数据库
var mockDB = map[string]string{
	"Tom":   "630",
	"Jack":  "589",
	"Sam":   "567",
	"Alice": "710",
	"Bob":   "420",
}

func main() {
	var port int
	var apiPort int
	flag.IntVar(&port, "port", 8001, "gRPC server port for peer communication")
	flag.IntVar(&apiPort, "api", 9001, "HTTP server port for user requests")
	flag.Parse()

	// 1. 初始化本地缓存节点 (gRPC 互联端口)
	addr := fmt.Sprintf("localhost:%d", port)
	server, err := lcache.NewServer(addr, "locache",
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal("创建缓存节点失败:", err)
	}

	// 2. 创建节点选择器
	picker, err := lcache.NewClientPicker(addr)
	if err != nil {
		log.Fatal("创建选择器失败:", err)
	}

	// 3. 创建缓存 Group 和 回源函数
	group := lcache.NewGroup("scores", 2<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("👉 [回源] 缓存未命中，当前机器 %s 正从数据库查询: %s", addr, key)
			time.Sleep(time.Millisecond * 500) // 模拟缓慢查询
			if v, ok := mockDB[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s 在数据库中不存在", key)
		}))

	group.RegisterPeers(picker)

	// 4. 启动 gRPC 节点（用于节点间通信）
	go func() {
		log.Printf("🚀 缓存对等节点 %s 启动 (gRPC)...\n", addr)
		if err := server.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	// 稍微等待让 etcd 注册完成
	time.Sleep(2 * time.Second)
	log.Printf("✅ 节点 %s 注册并就绪", addr)

	// 5. 启动给真实用户的 HTTP API 暴露接口（模拟业务调用）
	http.HandleFunc("/api/score", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		// 获取请求前的统计信息
		statsBefore := group.Stats()
		peerHitsBefore := statsBefore["peer_hits"].(int64)
		localHitsBefore := statsBefore["local_hits"].(int64)

		log.Printf("👋 收到用户请求查询 %s", key)
		view, err := group.Get(context.Background(), key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// 获取请求后的统计信息，对比差异
		statsAfter := group.Stats()
		peerHitsAfter := statsAfter["peer_hits"].(int64)
		localHitsAfter := statsAfter["local_hits"].(int64)

		if localHitsAfter > localHitsBefore {
			log.Printf("🟢 [数据来源] -> 从【本机内存缓存】直接读取！(本地命中)")
		} else if peerHitsAfter > peerHitsBefore {
			log.Printf("🔵 [数据来源] -> 通过【gRPC 从其他对等节点】获取！(远程命中)")
		} else {
			log.Printf("🔴 [数据来源] -> 本机和远程都没缓存，触发【真实数据库回源】加载！")
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"handled_by_api_port":"%d", "key":"%s", "score":"%s"}`+"\n", apiPort, key, view.String())))
	})

	apiAddr := fmt.Sprintf("localhost:%d", apiPort)
	log.Printf("🌐 用户 API 接口已启动: http://%s/api/score?key=Tom\n", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr, nil))
}
