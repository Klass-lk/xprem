// Package jobs runs background work on River, a Postgres-backed job queue.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
	"xprem/internal/database"
	"xprem/internal/database/postgres"
	"xprem/internal/repository"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

var ErrAlreadyRunning = errors.New("a job of this kind is already running for this scope")

// QueueBSDiff runs the bundle patch jobs. Each one holds two bundles and the
// patch in memory, so the queue stays narrow.
const QueueBSDiff = "bsdiff"

// QueueSourcemapIndex runs the source map index jobs. Each one decodes a
// whole map in memory, so one at a time.
const QueueSourcemapIndex = "sourcemap-index"

type Client struct {
	pool        *pgxpool.Pool
	workers     *river.Workers
	periodic    []*river.PeriodicJob
	riverClient *river.Client[pgx.Tx]
	// started is false for a Prepare client, which never works jobs.
	started bool
}

func NewClient(engine *database.Engine) (*Client, error) {
	pool, ok := engine.DB.(*pgxpool.Pool)
	if !ok {
		return nil, fmt.Errorf("the jobs client needs the pgx pool, got %T", engine.DB)
	}
	return &Client{pool: pool, workers: river.NewWorkers()}, nil
}

// Workers is the registry to add workers to, before Start.
func (c *Client) Workers() *river.Workers {
	return c.workers
}

// AddPeriodic schedules a job, before Start. River inserts it from one
// replica only, the elected leader.
func (c *Client) AddPeriodic(job *river.PeriodicJob) {
	c.periodic = append(c.periodic, job)
}

func (c *Client) migrate(ctx context.Context) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(c.pool), nil)
	if err != nil {
		return fmt.Errorf("failed to prepare the river migrator: %w", err)
	}
	release, err := postgres.AcquireAdvisoryLock(ctx, c.pool.Config().ConnConfig, postgres.RiverMigrationLockID, "river migration")
	if err != nil {
		return err
	}
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	release()
	if err != nil {
		return fmt.Errorf("failed to migrate the river schema: %w", err)
	}
	return nil
}

// newRiverClient builds a client for the registered workers. pollOnly skips
// LISTEN/NOTIFY, which needs a session a transaction pooler does not keep,
// and narrows the queues to fit a Lambda-sized connection pool.
func (c *Client) newRiverClient(pollOnly bool) (*river.Client[pgx.Tx], error) {
	queues := map[string]river.QueueConfig{
		river.QueueDefault:  {MaxWorkers: 10},
		QueueBSDiff:         {MaxWorkers: 2},
		QueueSourcemapIndex: {MaxWorkers: 1},
	}
	if pollOnly {
		queues[river.QueueDefault] = river.QueueConfig{MaxWorkers: 1}
		queues[QueueBSDiff] = river.QueueConfig{MaxWorkers: 1}
	}
	riverClient, err := river.NewClient(riverpgxv5.New(c.pool), &river.Config{
		Queues:       queues,
		Workers:      c.workers,
		PeriodicJobs: c.periodic,
		PollOnly:     pollOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build the river client: %w", err)
	}
	return riverClient, nil
}

func (c *Client) Start(ctx context.Context) error {
	if err := c.migrate(ctx); err != nil {
		return err
	}
	riverClient, err := c.newRiverClient(false)
	if err != nil {
		return err
	}
	if err := riverClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the river client: %w", err)
	}
	c.riverClient = riverClient
	c.started = true
	log.Println("🛠️  [JOBS] River job client started")
	return nil
}

// Prepare readies the client to enqueue without working any job: in scheduler
// mode the jobs are worked by Drain, one pass per scheduler invocation.
func (c *Client) Prepare(ctx context.Context) error {
	if err := c.migrate(ctx); err != nil {
		return err
	}
	riverClient, err := c.newRiverClient(true)
	if err != nil {
		return err
	}
	c.riverClient = riverClient
	log.Println("🛠️  [JOBS] River job client ready, jobs are worked by the scheduler")
	return nil
}

// ErrDrainIncomplete reports that jobs were still waiting when Drain ran out
// of time; the next pass picks them up.
var ErrDrainIncomplete = errors.New("jobs were still waiting when the drain ran out of time")

const (
	// drainBudget bounds a pass that has no deadline of its own (a long-lived
	// scheduler); a Lambda pass stops drainStopMargin before its deadline,
	// leaving room for running jobs to finish.
	drainBudget       = 5 * time.Minute
	drainStopMargin   = 90 * time.Second
	drainPollInterval = 2 * time.Second
)

// Drain works the queues until no job is due, then stops once the jobs in
// flight finish. A fresh River client per pass: a stopped client cannot be
// started again.
func (c *Client) Drain(ctx context.Context) error {
	deadline := time.Now().Add(drainBudget)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline.Add(-drainStopMargin)
	}
	riverClient, err := c.newRiverClient(true)
	if err != nil {
		return err
	}
	if err := riverClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the river client: %w", err)
	}
	pending, waitErr := c.waitForDueJobs(ctx, deadline)
	stopCtx, cancel := context.WithDeadline(context.Background(), deadline.Add(drainStopMargin/2))
	defer cancel()
	if err := riverClient.Stop(stopCtx); err != nil {
		// A pass must not return with the client still running: on Lambda it
		// would freeze mid-job. Cancelled jobs are retried by a later pass.
		log.Printf("[jobs] river client did not stop in time, cancelling running jobs: %v", err)
		cancelCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelStop()
		if err := riverClient.StopAndCancel(cancelCtx); err != nil {
			log.Printf("[jobs] river client did not stop cleanly: %v", err)
		}
	}
	if waitErr != nil {
		return waitErr
	}
	if pending > 0 {
		return fmt.Errorf("%w (%d due)", ErrDrainIncomplete, pending)
	}
	return nil
}

// waitForDueJobs returns once no job is due, or with the count still due at
// the deadline. Running jobs are left out: Stop waits for this client's, and
// one orphaned by a frozen invocation is River's rescuer's to retry.
func (c *Client) waitForDueJobs(ctx context.Context, deadline time.Time) (int64, error) {
	for {
		var due int64
		err := c.pool.QueryRow(ctx, `SELECT count(*) FROM river_job
			WHERE state = 'available' OR (state IN ('scheduled', 'retryable') AND scheduled_at <= now())`).Scan(&due)
		if err != nil {
			return 0, fmt.Errorf("failed to count due jobs: %w", err)
		}
		if due == 0 || !time.Now().Add(drainPollInterval).Before(deadline) {
			return due, nil
		}
		select {
		case <-ctx.Done():
			return due, ctx.Err()
		case <-time.After(drainPollInterval):
		}
	}
}

func (c *Client) Stop() {
	if c == nil || !c.started {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.riverClient.Stop(ctx); err != nil {
		log.Printf("[jobs] river client did not stop cleanly: %v", err)
	}
}

// Enqueue inserts args as a job; ErrAlreadyRunning when a unique constraint
// skipped the insert.
func (c *Client) Enqueue(ctx context.Context, args river.JobArgs) (string, error) {
	if c == nil || c.riverClient == nil {
		return "", repository.ErrNotSupportedInStatelessMode
	}
	inserted, err := c.riverClient.Insert(ctx, args, nil)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue the job: %w", err)
	}
	if inserted.UniqueSkippedAsDuplicate {
		return "", ErrAlreadyRunning
	}
	return strconv.FormatInt(inserted.Job.ID, 10), nil
}

func (c *Client) Get(ctx context.Context, jobId string) (*rivertype.JobRow, error) {
	if c == nil || c.riverClient == nil {
		return nil, nil
	}
	id, err := strconv.ParseInt(jobId, 10, 64)
	if err != nil {
		return nil, nil
	}
	job, err := c.riverClient.JobGet(ctx, id)
	if errors.Is(err, rivertype.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read the job: %w", err)
	}
	return job, nil
}

// LatestByArg returns the newest job of the kind whose args carry the value
// at the key, or nil.
func (c *Client) LatestByArg(ctx context.Context, kind string, argKey string, argValue string) (*rivertype.JobRow, error) {
	if c == nil || c.riverClient == nil {
		return nil, nil
	}
	params := river.NewJobListParams().
		Kinds(kind).
		Where("args->>@key = @value", river.NamedArgs{"key": argKey, "value": argValue}).
		OrderBy(river.JobListOrderByID, river.SortOrderDesc).
		First(1)
	listed, err := c.riverClient.JobList(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to list the scope's jobs: %w", err)
	}
	if len(listed.Jobs) == 0 {
		return nil, nil
	}
	return listed.Jobs[0], nil
}

// Cancel stops a job: immediately when it still waits, at its next context
// check when it runs. False means the job is unknown.
func (c *Client) Cancel(ctx context.Context, jobId string) (bool, error) {
	if c == nil || c.riverClient == nil {
		return false, nil
	}
	id, err := strconv.ParseInt(jobId, 10, 64)
	if err != nil {
		return false, nil
	}
	_, err = c.riverClient.JobCancel(ctx, id)
	if errors.Is(err, rivertype.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to cancel the job: %w", err)
	}
	return true, nil
}
