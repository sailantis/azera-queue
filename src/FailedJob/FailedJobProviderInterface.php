<?php

declare(strict_types=1);

namespace Azera\Queue\FailedJob;

/**
 * Persists permanently failed jobs for later inspection and replay.
 *
 * A job is moved here once the worker exhausts its {@see \Azera\Queue\JobInterface::tries()}
 * (or {@see \Azera\Queue\JobInterface::retryUntil()}) without success.
 * The CLI `queue:failed`/`queue:retry` commands read/replay via this
 * provider.
 */
interface FailedJobProviderInterface
{
    /**
     * Record a failed job.
     *
     * @param string      $queue     The queue the job failed on.
     * @param string      $payload   The serialized job.
     * @param \Throwable   $exception The exception that caused the failure.
     * @return string|int The identifier assigned to the failed job.
     */
    public function log(string $queue, string $payload, \Throwable $exception): string|int;

    /**
     * Return all failed jobs, newest first.
     *
     * @return array<array{id: string|int, queue: string, payload: string, exception: string, failed_at: int}>
     */
    public function all(): array;

    /**
     * Find a failed job by id.
     *
     * @param string|int $id
     * @return array{id: string|int, queue: string, payload: string, exception: string, failed_at: int}|null
     */
    public function find(string|int $id): ?array;

    /**
     * Forget (delete) a failed job by id.
     */
    public function forget(string|int $id): void;

    /**
     * Flush all failed jobs.
     */
    public function flush(): void;
}