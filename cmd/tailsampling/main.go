package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"skiresort/tail-sampling/internal/demo"
	"skiresort/tail-sampling/internal/pipeline"
	"skiresort/tail-sampling/internal/sim"
	"skiresort/tail-sampling/internal/sink"
	"skiresort/tail-sampling/internal/web"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP dashboard listen address")
	seed := flag.Uint64("seed", 20261125, "fixed demo traffic seed")
	flag.Parse()

	services := sim.Services()
	targets := sim.TargetMap(services)
	silence := map[string]time.Duration{}
	for _, svc := range services {
		if svc.Async {
			silence[svc.Name] = 6 * time.Second
		} else {
			silence[svc.Name] = 2 * time.Second
		}
	}

	cfg := pipeline.DefaultConfig()
	cfg.DefaultSilence = 2 * time.Second
	cfg.ServiceSilence = silence
	cfg.GracePeriod = 500 * time.Millisecond
	cfg.MaxExtensions = 1
	cfg.BatchSize = 64
	cfg.DecisionCacheCapacity = 2048
	cfg.MaxBufferBytes = 96 << 20
	cfg.LateAgeThreshold = 18 * time.Second
	cfg.RateWindowSeconds = 60
	cfg.AdjustInterval = 3 * time.Second
	cfg.BudgetHorizon = 90 * time.Second
	cfg.Targets = targets

	pipe := pipeline.New(cfg, sink.NopSink{})
	runner := demo.New(pipe, *seed)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runner.Start(ctx)

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				pipe.FlushAll(time.Now())
				return
			case now := <-ticker.C:
				pipe.Sweep(now)
			}
		}
	}()

	server := web.NewServer(pipe)
	listenAddr := normalizeAddr(*addr)
	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	fmt.Printf("滑雪场尾部采样服务已启动: http://localhost%s\n", listenAddr)
	fmt.Println("模拟流量运行中；按 Ctrl+C 退出。完整三小时核对: go run ./cmd/verify")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return addr
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return ":" + addr
}
