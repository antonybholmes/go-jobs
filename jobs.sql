 
-- function to automatically update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = now();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    email text NOT NULL,
    name text,
    description text,
    -- The type of job (e.g., "email", "crispr")
    type text NOT NULL,
    status text NOT NULL DEFAULT 'submitted',
    -- The S3 bucket where the object is stored
    bucket text NOT NULL,
    -- The key of the object in the S3 bucket
    object_key text NOT NULL,
    -- The original filename of the object
    filename text NOT NULL,
    -- The MIME type of the object
    content_type text NOT NULL,
    -- The size of the object in bytes
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    -- The timestamp when the job was created
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TRIGGER update_jobs_updated_at
BEFORE UPDATE ON jobs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- CREATE TABLE IF NOT EXISTS job_outbox (
--     id uuid PRIMARY KEY,
--     job_id uuid NOT NULL REFERENCES jobs(id),
--     payload jsonb NOT NULL,
--     created_at timestamptz NOT NULL,
--     published_at timestamptz
-- );

-- CREATE INDEX IF NOT EXISTS job_outbox_pending_idx
--     ON job_outbox (job_id, created_at) WHERE published_at IS NULL;
