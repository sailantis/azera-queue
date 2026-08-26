# azera-queue

Queue companion for the [Azera framework](../azera-framework).

The framework ships queue **contracts** (`Azera\Queue\QueueInterface`, `JobInterface`, `Job`, `SyncQueue`, `QueueException`) and the docs defer async backends to a companion. This package provides:

- `Azera\Queue\Serialization\JobSerializerInterface` + `NativeSerializer` — serialize/deserialize jobs for async backends, with a class-allowlist for safety.
- `Azera\Queue\Backend\DatabaseQueue` — a SQL-backed backend using the framework's `Database`/`Query` (no extra extension required). Default for apps already on MySQL.
- `Azera\Queue\Backend\RedisQueue` — `ext-redis`-backed queue with at-least-once delivery, visibility timeouts, delayed jobs, priorities.
- `Azera\Queue\Backend\AmqpQueue` — RabbitMQ queue that works with **either** `ext-amqp` (PECL extension) or `php-amqplib` (pure-PHP library), via a transport adapter. Use `AmqpQueue::wire($connection)` to auto-detect the installed implementation.
- `Azera\Queue\Backend\FileQueue` — a JSON-file-backed backend needing nothing beyond core PHP. Ideal for development, single-instance deployments and cron-driven workers; jobs persist across restarts.
- `Azera\Queue\Worker\Worker` — long-running pop → deserialize → `handle()` → ack loop honoring `tries()`, `backoff()`, `retryUntil()`.
- `Azera\Queue\Task\QueueTask` — `azera queue:work|failed|retry|flush` CLI task extending the framework's `Azera\Cli\Task`.
- `Azera\Queue\FailedJob\FailedJobProviderInterface` + DB/Redis implementations — persist permanently failed jobs for replay.
- `Azera\Queue\Dispatch\Dispatcher` — fluent `dispatch($job)->onQueue()->delay()->priority()`.
- `Azera\Queue\Event\QueueEvents` — PSR-14 `JobPushed`, `JobReserved`, `JobProcessed`, `JobFailed`, `JobRetried`, `JobReleased`.

## Installation

```json
{
    "repositories": [{ "type": "path", "url": "../azera-queue" }],
    "require": { "sailantis/azera-queue": "dev-main" }
}
```

## Database queue schema

```sql
CREATE TABLE azera_jobs (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    queue       VARCHAR(64) NOT NULL,
    payload     LONGTEXT NOT NULL,
    attempts    INT UNSIGNED NOT NULL DEFAULT 0,
    available_at BIGINT UNSIGNED NOT NULL,
    reserved_at  BIGINT UNSIGNED NULL,
    created_at   BIGINT UNSIGNED NOT NULL,
    dedup_key    VARCHAR(255) NULL
);
CREATE INDEX idx_azera_jobs_queue_reserved ON azera_jobs (queue, reserved_at);
```

See `src/Backend/migrations/` for the full migration.

## File queue

`FileQueue` stores jobs as a single JSON document under a directory you supply, plus an advisory lock file. It needs no database or extension, so it's handy for shared hosting, single-instance apps and cron-driven workers.

```php
use Azera\Queue\Backend\FileQueue;

$queue = new FileQueue(__DIR__ . '/storage/queue');
```

Mutations run under an exclusive `flock()` and are written atomically (temp file + rename), so several worker processes on the *same host* are safe. It is **not** designed for concurrent writers on a shared network filesystem, nor for horizontal scale-out — reach for `DatabaseQueue`/`RedisQueue`/`AmqpQueue` there.

## License

MIT.

MIT.