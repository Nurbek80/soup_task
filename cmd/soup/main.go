package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"soupqueue/internal/api"
	"soupqueue/internal/store"
	"soupqueue/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) != 2 || (os.Args[1] != "api" && os.Args[1] != "worker" && os.Args[1] != "migrate") {
		fmt.Fprintln(os.Stderr, "usage: soup api|worker|migrate")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	databaseURL := getenv("DATABASE_URL", "postgres://soup:soup@localhost:55432/soup?sslmode=disable")
	s, err := store.Open(ctx, databaseURL)
	fatal(log, err)
	defer s.Close()
	fatal(log, s.Migrate(ctx))
	switch os.Args[1] {
	case "migrate":
		log.Info("migration complete")
	case "api":
		server := &http.Server{Addr: getenv("HTTP_ADDR", ":8080"), Handler: (&api.API{Store: s, Log: log}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
		log.Info("api started", "address", server.Addr)
		err = server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			fatal(log, err)
		}
	case "worker":
		lease := time.Duration(getenvInt("LEASE_MS", 3000)) * time.Millisecond
		id := getenv("WORKER_ID", hostname()+"-"+strconv.Itoa(os.Getpid()))
		err = (&worker.Worker{Store: s, ID: id, Lease: lease, Poll: 50 * time.Millisecond, Log: log}).Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			fatal(log, err)
		}
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func getenvInt(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err == nil {
		return v
	}
	return d
}
func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "worker"
	}
	return h
}
func fatal(log *slog.Logger, err error) {
	if err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}
