<?php

declare(strict_types=1);

namespace Azera\Queue\Backend\Contract;

/**
 * Marker interface for queue backends that support explicit reservation.
 *
 * The framework's {@see \Azera\Queue\QueueInterface} is a minimal push/
 * registerWorker contract. Async backends additionally need a way to
 * reserve a job for processing and release/delete it afterwards; this
 * interface captures those operations so the {@see \Azera\Queue\Worker\Worker}
 * can drive any reservable backend uniformly.
 */
interface ReservableQueueInterface
{
    /**
     * Reserve the next available job, returning a row with at least
     * `id`, `payload`, and `attempts` keys, or null when the queue is
     * empty.
     *
     * @return array{id: string|int, payload: string, attempts: int}|null
     */
    public function reserve(string $queue = 'default'): ?array;

    /**
     * Delete a processed job by id.
     */
    public function delete(string|int $id): void;

    /**
     * Release a reserved job back to the queue after $delay seconds.
     */
    public function release(string|int $id, int $delay = 0): void;

    /**
     * Count jobs on the queue.
     */
    public function count(string $queue = 'default'): int;
}