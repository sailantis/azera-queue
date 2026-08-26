<?php

declare(strict_types=1);

namespace Azera\Queue\FailedJob;

/**
 * In-memory failed-job provider for tests and short-lived processes.
 */
class ArrayFailedJobProvider implements FailedJobProviderInterface
{
    /** @var array<array{id: int, queue: string, payload: string, exception: string, failed_at: int}> */
    private array $jobs = [];

    private int $nextId = 1;

    public function log(string $queue, string $payload, \Throwable $exception): string|int
    {
        $id = $this->nextId++;
        $this->jobs[] = [
            'id'        => $id,
            'queue'     => $queue,
            'payload'   => $payload,
            'exception' => (string) $exception,
            'failed_at' => time(),
        ];
        return $id;
    }

    public function all(): array
    {
        return array_reverse($this->jobs);
    }

    public function find(string|int $id): ?array
    {
        $id = (int) $id;
        foreach ($this->jobs as $job) {
            if ($job['id'] === $id) {
                return $job;
            }
        }
        return null;
    }

    public function forget(string|int $id): void
    {
        $id = (int) $id;
        foreach ($this->jobs as $i => $job) {
            if ($job['id'] === $id) {
                unset($this->jobs[$i]);
                return;
            }
        }
    }

    public function flush(): void
    {
        $this->jobs = [];
    }
}