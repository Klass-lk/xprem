package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	"xprem/config"
	"xprem/internal/bucketmigration"
	"xprem/internal/jobs"
	"xprem/internal/metrics"
	infrastructure "xprem/internal/router"

	_ "xprem/internal/bucketmigrations"

	"github.com/gin-gonic/gin"
	"github.com/klass-lk/ginboot"
	lambdarunner "github.com/klass-lk/ginboot/runtime/lambda"
)

func init() {
	config.LoadConfig()
	metrics.InitMetrics()
}

// bootHandler answers while the bucket migrations run, and splits the two probes
// on purpose. /hc (liveness) is registered below and answers 200 throughout, so
// the orchestrator does not kill a pod in the middle of a long migration.
// /ready (readiness) is deliberately NOT registered: it falls into the catch-all
// and answers 503 + Retry-After, which drops the pod from the Service endpoints
// without killing it, so nothing is served from a half-migrated bucket. Every
// other request gets that same 503. Once main swaps in the real router, /ready
// answers 200 like /hc (see internal/router/router.go).
func bootHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/hc", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "15")
		http.Error(w, "storage migration in progress, updates are on hold", http.StatusServiceUnavailable)
	})
	return mux
}

// scheduledWorker registers one xprem background task as a ginboot cron
// worker, which /_ginboot/workers lists for the platform to schedule.
type scheduledWorker struct {
	task infrastructure.ScheduledTask
}

func (w scheduledWorker) Name() string { return w.task.Name }
func (w scheduledWorker) Cron() string { return w.task.Cron }

// Interval is unused: ginboot schedules a worker with a Cron() on that.
func (w scheduledWorker) Interval() time.Duration { return time.Minute }

func (w scheduledWorker) Execute(ctx context.Context) error {
	err := w.task.Run(ctx)
	if errors.Is(err, jobs.ErrDrainIncomplete) {
		// Unfinished rather than failed: the next pass resumes the queue.
		return fmt.Errorf("%w: %w", ginboot.ErrIncomplete, err)
	}
	return err
}

// initXprem runs the storage migrations and builds the xprem router, with
// every background task registered on the ginboot scheduler.
func initXprem(server *ginboot.Server) (http.Handler, func()) {
	// The lock is released on failure, so a crash-looping pod retries the
	// migration on every boot instead of skipping it while the lock expires.
	if err := bucketmigration.EnsureMigrations(); err != nil {
		log.Fatalf("🚨 [BUCKET] %v", err)
	}
	container, cleanup := infrastructure.InitDependencies(context.Background())
	for _, task := range container.ScheduledTasks {
		server.RegisterWorkerStruct(scheduledWorker{task: task})
	}
	// No global CORS: each surface declares its own where its routes are
	// registered (dashboard subrouters, OAuth endpoints, /mcp).
	return infrastructure.NewRouter(container), cleanup
}

func main() {
	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	server := ginboot.New()

	var handler atomic.Pointer[http.Handler]
	boot := bootHandler()
	handler.Store(&boot)
	// ginboot answers its own routes (/health, /healthz, /openapi.json,
	// /_ginboot/*); every other request is xprem's.
	server.Engine().NoRoute(func(c *gin.Context) {
		// gin presets 404 on a NoRoute request; an xprem handler that writes
		// a body without calling WriteHeader means 200.
		c.Status(http.StatusOK)
		(*handler.Load()).ServeHTTP(c.Writer, c.Request)
	})

	port, err := strconv.Atoi(config.GetPort())
	if err != nil {
		log.Fatalf("Invalid PORT %q: %v", config.GetPort(), err)
	}

	if config.IsLambda() {
		// Each invocation is one request or one scheduled worker, so
		// everything is ready before the runtime accepts the first.
		ready, cleanup := initXprem(server)
		defer cleanup()
		handler.Store(&ready)
		server.SetRunner(lambdarunner.NewRunnerFor(server))
		log.Fatalf("Lambda runtime stopped: %v", server.Start(port))
	}

	httpServer := &http.Server{
		Addr:              net.JoinHostPort(config.GetBindAddress(), config.GetPort()),
		Handler:           server.Engine(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	// A runner keeps BIND_TO_ADDRESS and the timeouts, and lets the port bind
	// before the migrations run so /hc answers from the very first probe;
	// Start only returns on failure.
	server.SetRunner(func(*gin.Engine) error { return httpServer.ListenAndServe() })
	go func() {
		log.Fatalf("Server failed to start: %v", server.Start(port))
	}()
	log.Println("Server is running on " + httpServer.Addr)

	ready, cleanup := initXprem(server)
	defer cleanup()
	// Start skips the scheduler when a runner is set, and the workers only
	// exist once the dependencies are built.
	if len(server.Scheduler().GetTasks()) > 0 {
		server.Scheduler().Start(context.Background())
	}
	handler.Store(&ready)
	log.Println("✅ Server is ready to serve traffic.")
	select {}
}
