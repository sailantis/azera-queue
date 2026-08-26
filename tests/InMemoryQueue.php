<?php

declare(strict_types=1);

namespace Azera\Queue\Tests;

use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\QueueInterface;

/**
 * In-memory reservable queue for tests.
 *
 * Mimics the DatabaseQueue's reserve/delete/release contract so the
 * Worker can be exercised without a real database.
 */
final class InMemoryQueue implements QueueInterface, ReservableQueueInterface
{
    /** @var array<int, array{id: int, payload: string, attempts: int, available_at: int, reserved: bool}> */
    private array $jobs = [];

    private int $nextId = 1;

    public function push(\Azera\Queue\JobInterface $job, array $options = []): mixed
    {
        $id = $this->nextId++;
        $this->jobs[] = [
            'id'           => $id,
            'payload'      => serialize($job),
            'attempts'     => 0,
            'available_at' => time() + (int) ($options['delay'] ?? 0),
            'reserved'     => false,
        ];
        return $id;
    }

    public function registerWorker(string $jobClass, ?callable $handler = null): void {}

    public function reserve(string $queue = 'default'): ?array
    {
        $now = time();
        foreach ($this->jobs as $i => &$job) {
            if (!$job['reserved'] && $job['available_at'] <= $now) {
                $job['reserved'] = true;
                $job['attempts']++;
                return ['id' => $job['id'], 'payload' => $job['payload'], 'attempts' => $job['attempts']];
            }
        }
        unset($job);

        return null;
    }

    public function delete(string|int $id): void
    {
        foreach ($this->jobs as $i => $job) {
            if ($job['id'] === (int) $id) {
                unset($this->jobs[$i]);
                $this->jobs = array_values($this->jobs);
                return;
            }
        }
    }

    public function release(string|int $id, int $delay = 0): void
    {
        foreach ($this->jobs as &$job) {
            if ($job['id'] === (int) $id) {
                $job['reserved']     = false;
                $job['available_at'] = time() + $delay;
            }
        }
        unset($job);
    }

    public function count(string $queue = 'default'): int
    {
        return count($this->jobs);
    }
}