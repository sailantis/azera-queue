-- azera_jobs table for the DatabaseQueue backend.
-- Apply with: azera db:execute azera_jobs

CREATE TABLE IF NOT EXISTS azera_jobs (
    id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    queue         VARCHAR(64)  NOT NULL,
    payload       LONGTEXT     NOT NULL,
    attempts      INT UNSIGNED NOT NULL DEFAULT 0,
    available_at  BIGINT UNSIGNED NOT NULL,
    reserved_at   BIGINT UNSIGNED NULL,
    created_at    BIGINT UNSIGNED NOT NULL,
    -- Optional deduplication key (JobInterface::id()). NULL when dedup is disabled.
    dedup_key     VARCHAR(255) NULL
);

CREATE INDEX IF NOT EXISTS idx_azera_jobs_queue_reserved
    ON azera_jobs (queue, reserved_at);

CREATE INDEX IF NOT EXISTS idx_azera_jobs_available
    ON azera_jobs (available_at);

-- Unique per dedup_key so pushing the same job id twice is idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS uq_azera_jobs_dedup_key
    ON azera_jobs (dedup_key);