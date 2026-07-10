// nomios runs the Nomios CDC node: it loads hyperloop definitions from a
// YAML config, starts them, and serves the HTTP control plane.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/duy-tung/cdc-scaling/pkg/config"
	"github.com/duy-tung/cdc-scaling/pkg/server"
)

func main() {
	var (
		configPath = flag.String("config", "configs/nomios.yaml", "path to YAML config")
		listen     = flag.String("listen", "", "HTTP listen address (overrides config)")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}

	mgr := server.NewManager(log)
	for i := range cfg.Hyperloops {
		def := &cfg.Hyperloops[i]
		hl, err := def.Build(log)
		if err != nil {
			log.Error("failed to build hyperloop", "err", err)
			os.Exit(1)
		}
		if err := mgr.Register(hl); err != nil {
			log.Error("failed to register hyperloop", "err", err)
			os.Exit(1)
		}
		if def.AutoStartEnabled() {
			if err := mgr.Start(hl.ID()); err != nil {
				log.Error("failed to start hyperloop", "hyperloop", hl.ID(), "err", err)
				os.Exit(1)
			}
			log.Info("hyperloop started", "hyperloop", hl.ID())
		}
	}

	httpSrv := &http.Server{Addr: cfg.Listen, Handler: mgr.Handler()}
	go func() {
		log.Info("control plane listening", "addr", cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mgr.StopAll(ctx)
	_ = httpSrv.Shutdown(ctx)
	log.Info("bye")
}
