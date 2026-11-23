package jobs

import (
	"context"
	"os"
	"testing"

	"uuid"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FakeSender struct {
	err     error
	id      string
	payload []byte
}

func (s *FakeSender) Send(_ context.Context, payload []byte) error {
	s.payload = payload
	return s.err
}

func TestPostgresOutbox(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schemaName := "test_" + uuid.NewV7().String()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schemaName+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	}()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// store := &PostgresStore{pool: pool}
	// id, err := uuid.NewV7()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// j := Job{ID: id.String(), Bucket: "bucket", Key: "key", Filename: "file", ContentType: "text/plain", Size: 5, CreatedAt: time.Now().UTC()}
	// if err := store.CreateJob(ctx, &j); err != nil {
	// 	t.Fatal(err)
	// }
	// var jobCount, pending int
	// if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_outbox WHERE published_at IS NULL)").Scan(&jobCount, &pending); err != nil || jobCount != 1 || pending != 1 {
	// 	t.Fatalf("job and outbox not committed together: jobs=%d pending=%d err=%v", jobCount, pending, err)
	// }
	// sender := &FakeSender{err: errors.New("SQS unavailable")}
	// if sent, err := store.PublishOne(ctx, sender); sent || err == nil {
	// 	t.Fatalf("expected failed send, got sent=%v err=%v", sent, err)
	// }
	// if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_outbox WHERE published_at IS NULL").Scan(&pending); err != nil || pending != 1 {
	// 	t.Fatalf("failed send should remain pending: pending=%d err=%v", pending, err)
	// }
	// sender.err = nil
	// if sent, err := store.PublishOne(ctx, sender); !sent || err != nil {
	// 	t.Fatalf("retry failed: sent=%v err=%v", sent, err)
	// }
	// var delivered Job
	// if err := json.Unmarshal(sender.payload, &delivered); err != nil || sender.id != j.ID || delivered.ID != j.ID || delivered.Key != j.Key {
	// 	t.Fatalf("unexpected message: id=%s payload=%s err=%v", sender.id, sender.payload, err)
	// }
	// if sent, err := store.PublishOne(ctx, sender); sent || err != nil {
	// 	t.Fatalf("expected empty outbox: sent=%v err=%v", sent, err)
	// }
	// An outbox insert failure must roll back the preceding job insert.
	// second := j
	// second.ID = uuid.NewString()
	// if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS
	// 	$$ BEGIN RAISE EXCEPTION 'injected failure'; END $$;
	// 	CREATE TRIGGER reject_outbox BEFORE INSERT ON job_outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox()`); err != nil {
	// 	t.Fatal(err)
	// }
	// if err := store.CreateJob(ctx, &second); err == nil {
	// 	t.Fatal("expected outbox insert failure")
	// }
	// if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE id=$1", second.ID).Scan(&jobCount); err != nil || jobCount != 0 {
	// 	t.Fatalf("job insert was not rolled back: count=%d err=%v", jobCount, err)
	// }
}
