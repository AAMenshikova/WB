package main

import (
	"awesomeProject/kafka"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"awesomeProject/internal/api"
	"awesomeProject/internal/cache"
	"awesomeProject/internal/db"
	"awesomeProject/internal/repository"
	"awesomeProject/internal/service"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	sqlDB, err := db.InitDB(logger)
	if err != nil {
		logger.Error("db init failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer sqlDB.Close()
	repo := repository.NewRepository(sqlDB)
	redisCache, err := cache.NewCache(logger)
	if err != nil {
		logger.Error("redis init failed", slog.Any("err", err))
		os.Exit(1)
	}
	svc := service.NewService(repo, redisCache, logger)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := svc.Warmup(ctx); err != nil {
			logger.Error("cache warmup failed", slog.Any("err", err))
		}
		cancel()
	}
	consumer := kafka.NewConsumer(svc, logger)
	kctx, kcancel := context.WithCancel(context.Background())
	go func() {
		if err := consumer.Run(kctx); err != nil {
			logger.Error("kafka consumer stopped with error", slog.Any("err", err))
		}
	}()
	srv := &http.Server{
		Addr:    ":" + getEnv("HTTP_PORT", "8081"),
		Handler: buildMux(svc),
	}
	go func() {
		logger.Info("http server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", slog.Any("err", err))
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("shutting down...")
	kcancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(ctx)
	cancel()
}

func buildMux(svc *service.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/order/", api.HandlerGet(svc))
	mux.HandleFunc("/order", api.HandlerPost(svc))
	webDir := getEnv("WEB_DIR", "./web")
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	return mux
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
