<?php

declare(strict_types=1);

namespace Azera\Queue\Backend;

use Azera\Db\Database;
use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\Event\JobPushed;
use Azera\Queue\QueueException;
use Azera\Queue\QueueInterface;
use Azera\Queue\Serialization\JobSerializerInterface;
use Azera\Queue\Serialization\NativeSerializer;
use PDOException;
use PDOStatement;
use Psr\EventDispatcher\EventDispatcherInterface;

/**
 * SQL-backed queue using the framework's {@see Database} connection.
 *
 * The default backend for applications already running on MySQL/Postgres
 * — no extra extension required. Jobs are stored in an `azera_jobs`
 * table (see `src/Backend/migrations/`).
 *
 * Delivery is at-least-once: a worker reserves a job by setting
 * `reserved_at`, processes it, and deletes the row on success. If the
 * worker dies mid-processing, the visibility timeout allows the job
 * to be re-reserved after `reserveFor` seconds.
 *
 * Delayed jobs are stored with an `available_at` in the future; the
 * worker skips rows whose `available_at > NOW()`. Priorities map to
 * multiple queue names (e.g. `default`, `high`).
 */
class DatabaseQueue implements QueueInterface, ReservableQueueInterface
{
    /**
     * @param Database                       $db          The database connection.
     * @param string                          $table       Jobs table name (default `azera_jobs`).
     * @param JobSerializerInterface          $serializer  Job serializer (default NativeSerializer).
     * @param EventDispatcherInterface|null   $dispatcher  Optional PSR-14 dispatcher.
     * @param int                             $reserveFor  Seconds a reserved job is held before it can be re-reserved (default 60).
     */
    public function __construct(
        private Database $db,
        private string $table = 'azera_jobs',
        private JobSerializerInterface $serializer = new NativeSerializer(),
        private ?EventDispatcherInterface $dispatcher = null,
        private int $reserveFor = 60,
    ) {}

    /**
     * Options understood by this backend. Anything else is rejected so a
     * misconfigured dispatcher fails loudly instead of silently dropping
     * settings.
     */
    private const SUPPORTED_OPTIONS = ['queue', 'delay'];

    public function push(\Azera\Queue\JobInterface $job, array $options = []): mixed
    {
        $unknown = array_diff(array_keys($options), self::SUPPORTED_OPTIONS);
        if ($unknown !== []) {
            throw new QueueException(sprintf(
                'DatabaseQueue does not support option(s): %s. Supported: %s.',
                implode(', ', $unknown),
                implode(', ', self::SUPPORTED_OPTIONS),
            ));
        }

        $queue       = $options['queue'] ?? $job->queue();
        $delay       = (int) ($options['delay'] ?? 0);
        $availableAt = time() + $delay;

        $payload = $this->serializer->serialize($job);

        $jobId = $job->id();

        try {
            if ($jobId === null) {
                $this->db->query(
                    "INSERT INTO {$this->table} (queue, payload, attempts, available_at, reserved_at, created_at, dedup_key) VALUES (?, ?, 0, ?, NULL, ?, NULL)",
                    [$queue, $payload, $availableAt, time()],
                );
            } else {
                $this->db->query(
                    "INSERT INTO {$this->table} (queue, payload, attempts, available_at, reserved_at, created_at, dedup_key) VALUES (?, ?, 0, ?, NULL, ?, ?)",
                    [$queue, $payload, $availableAt, time(), $jobId],
                );
            }
        } catch (PDOException $e) {
            // Duplicate dedup_key (unique index) → the job is already queued.
            // Deduplication: return the existing job's id rather than fail.
            if ($jobId !== null && $this->isDuplicateKeyError($e)) {
                $existing = $this->db->query(
                    "SELECT id FROM {$this->table} WHERE dedup_key = ? LIMIT 1",
                    [$jobId],
                );
                $row = $existing instanceof PDOStatement ? $existing->fetch(\PDO::FETCH_ASSOC) : false;

                if (is_array($row) && isset($row['id'])) {
                    $this->dispatcher?->dispatch(new JobPushed($queue, (string) $row['id'], $job::class));
                    return $row['id'];
                }
            }

            throw new QueueException('Failed to enqueue job: ' . $e->getMessage(), 0, $e);
        }

        $id = $this->db->lastInsertId();

        $this->dispatcher?->dispatch(new JobPushed($queue, is_string($id) ? $id : null, $job::class));

        return $id;
    }

    /**
     * Whether a PDO exception is a duplicate-key / unique-violation error.
     */
    private function isDuplicateKeyError(PDOException $e): bool
    {
        $code = $e->errorInfo[1] ?? null;

        // MySQL / MariaDB: 1062. PostgreSQL: 23505. SQLite: 19 / 2067.
        return in_array((int) $code, [1062, 23505, 19], true);
    }

    public function registerWorker(string $jobClass, ?callable $handler = null): void
    {
        // The DatabaseQueue does not call workers directly; the Worker
        // task resolves handlers. This method is a no-op for parity with
        // the interface (the SyncQueue uses it, async backends ignore it).
    }

    /**
     * Reserve the next available job on $queue, marking it as reserved.
     *
     * Returns the raw row (id, payload, attempts) or null when no job
     * is available. The Worker deserializes the payload and runs it.
     *
     * @return array{id: string|int, payload: string, attempts: int}|null
     */
    public function reserve(string $queue = 'default'): ?array
    {
        $this->db->begin();

        try {
            // Lock the selected row for the duration of the transaction so
            // two concurrent workers cannot claim the same job (MySQL
            // `FOR UPDATE`, Postgres `SKIP LOCKED`). On SQLite, writers are
            // serialized so the plain SELECT is sufficient.
            $row = $this->fetchNext($queue, true);

            if ($row === null) {
                $this->db->rollback();
                return null;
            }

            $this->db->query(
                "UPDATE {$this->table} SET reserved_at = ?, attempts = attempts + 1 WHERE id = ?",
                [time(), $row['id']],
            );

            $this->db->commit();

            return $row;
        } catch (\Throwable $e) {
            $this->db->rollback();
            throw new QueueException('Failed to reserve job: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Delete a processed job.
     */
    public function delete(string|int $id): void
    {
        $this->db->query("DELETE FROM {$this->table} WHERE id = ?", [$id]);
    }

    /**
     * Release a reserved job back to the queue after $delay seconds.
     */
    public function release(string|int $id, int $delay = 0): void
    {
        $this->db->query(
            "UPDATE {$this->table} SET reserved_at = NULL, available_at = ? WHERE id = ?",
            [time() + $delay, $id],
        );
    }

    /**
     * Count all rows on $queue (available, delayed, and reserved/in-flight).
     */
    public function count(string $queue = 'default'): int
    {
        $stmt = $this->db->query(
            "SELECT COUNT(*) FROM {$this->table} WHERE queue = ?",
            [$queue],
        );

        if ($stmt instanceof PDOStatement) {
            return (int) $stmt->fetchColumn();
        }

        return 0;
    }

    /**
     * Count jobs that are immediately claimable: not delayed and not
     * reserved (or past their visibility timeout). Useful for monitoring
     * a genuine backlog, excluding in-flight jobs.
     */
    public function countAvailable(string $queue = 'default'): int
    {
        $now       = time();
        $expiredAt = $now - $this->reserveFor;

        $stmt = $this->db->query(
            "SELECT COUNT(*) FROM {$this->table}
             WHERE queue = ?
               AND available_at <= ?
               AND (reserved_at IS NULL OR reserved_at <= ?)",
            [$queue, $now, $expiredAt],
        );

        if ($stmt instanceof PDOStatement) {
            return (int) $stmt->fetchColumn();
        }

        return 0;
    }

    /**
     * Fetch the next available row for $queue, honoring the visibility
     * timeout for reserved-but-stale jobs.
     *
     * When $forUpdate is true, a row lock is added so concurrent workers
     * cannot claim the same job (MySQL `FOR UPDATE`, Postgres `SKIP LOCKED`).
     */
    private function fetchNext(string $queue, bool $forUpdate = false): ?array
    {
        $now       = time();
        $expiredAt = $now - $this->reserveFor;

        $lockSql = '';
        if ($forUpdate) {
            $driver = $this->db->getDriver();
            if ($driver === 'mysql') {
                $lockSql = ' FOR UPDATE';
            } elseif ($driver === 'pgsql') {
                // SKIP LOCKED lets other workers immediately take other
                // jobs instead of blocking on the locked row.
                $lockSql = ' FOR UPDATE SKIP LOCKED';
            }
            // sqlite: no row-level locking; writers are serialized anyway.
        }

        $stmt = $this->db->query(
            "SELECT id, payload, attempts FROM {$this->table}
             WHERE queue = ?
               AND available_at <= ?
               AND (reserved_at IS NULL OR reserved_at <= ?)
             ORDER BY available_at ASC
             LIMIT 1{$lockSql}",
            [$queue, $now, $expiredAt],
        );

        if (!$stmt instanceof PDOStatement) {
            return null;
        }

        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        if (!is_array($row)) {
            return null;
        }

        return [
            'id'       => $row['id'],
            'payload'  => (string) $row['payload'],
            'attempts' => (int) $row['attempts'],
        ];
    }
}