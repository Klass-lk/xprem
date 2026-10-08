// Drain is the scheduler-mode job runner: one pass per scheduler invocation.
// Needs a real Postgres and skips without TEST_DATABASE_URL:
//
//	docker run -d --name eoo-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
//	TEST_DATABASE_URL="postgres://postgres:test@localhost:55432/postgres?sslmode=disable" go test ./internal/jobs/
package jobs

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
	"xprem/internal/database"
	"xprem/internal/database/postgres/pgdb"
	"xprem/internal/database/postgres/pgtest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.RunSerialized(m))
}

type drainTestArgs struct {
	N int `json:"n"`
}

func (drainTestArgs) Kind() string { return "drain_test" }

type drainTestWorker struct {
	river.WorkerDefaults[drainTestArgs]
	worked  *atomic.Int32
	release chan struct{}
}

func (w *drainTestWorker) Work(ctx context.Context, _ *river.Job[drainTestArgs]) error {
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.worked.Add(1)
	return nil
}

func newDrainTestClient(t *testing.T, worker *drainTestWorker) (*Client, *pgxpool.Pool) {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL must be set in CI")
		}
		t.Skip("TEST_DATABASE_URL not set; start a Postgres and set it to run the jobs tests")
	}
	pool, err := pgxpool.New(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	client, err := NewClient(&database.Engine{Queries: pgdb.New(pool), DB: pool})
	require.NoError(t, err)
	river.AddWorker(client.Workers(), worker)
	require.NoError(t, client.Prepare(context.Background()))
	// Jobs left by other packages would count as due here.
	_, err = pool.Exec(context.Background(), "DELETE FROM river_job")
	require.NoError(t, err)
	return client, pool
}

func TestDrainWorksEveryDueJob(t *testing.T) {
	worked := &atomic.Int32{}
	client, pool := newDrainTestClient(t, &drainTestWorker{worked: worked})
	ctx := context.Background()

	for n := range 3 {
		_, err := client.Enqueue(ctx, drainTestArgs{N: n})
		require.NoError(t, err)
	}
	// Prepare only enqueues: nothing is worked before a drain.
	require.Zero(t, worked.Load())

	require.NoError(t, client.Drain(ctx))
	require.EqualValues(t, 3, worked.Load())

	var completed int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM river_job WHERE kind = 'drain_test' AND state = 'completed'").Scan(&completed))
	require.Equal(t, 3, completed)
}

func TestDrainReportsJobsLeftAtDeadline(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	client, _ := newDrainTestClient(t, &drainTestWorker{worked: &atomic.Int32{}, release: release})

	for n := range 2 {
		_, err := client.Enqueue(context.Background(), drainTestArgs{N: n})
		require.NoError(t, err)
	}
	// A Lambda-style deadline already inside the stop margin: the pass must
	// give up at once and say the queue is not empty.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Drain(ctx)
	require.True(t, errors.Is(err, ErrDrainIncomplete), "got %v", err)
}
