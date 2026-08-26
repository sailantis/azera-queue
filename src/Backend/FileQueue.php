<?php

declare(strict_types=1);

namespace Azera\Queue\Backend;

use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\Event\JobPushed;
use Azera\Queue\JobInterface;
use Azera\Queue\QueueException;
use Azera\Queue\QueueInterface;
use Azera\Queue\Serialization\JobSerializerInterface;
use Azera\Queue\Serialization\NativeSerializer;
use Psr\EventDispatcher\EventDispatcherInterface;

/**
 * File/JSON-backed queue using a single store document plus an advisory
 * lock file.
 *
 * Intended for development, single-instance deployments, cron-driven
 * workers, and any environment without a database or a cache extension.
 * It needs nothing beyond core PHP: jobs persist across restarts, so
 * delayed jobs, retries and visibility timeouts survive a process death.
 *
 * Concurrency model
 * -----------------
 * All mutations (push, reserve, delete, release) run under an exclusive
 * `flock()` on a per-directory lock file, and the JSON store is written
 * atomically (temp file + rename). This makes the backend safe for
 * multiple worker processes on the *same host*. It does NOT protect
 * against concurrent writers on a shared network filesystem (e.g. NFS),
 * and the lock is advisory — a job may be seen twice only if the worker
 * dies mid-processing, which the visibility timeout already handles
 * (at-least-once delivery, same as {@see DatabaseQueue}).
 *
 * @see https://www.php.net/manual/en/function.flock.php
 */
class FileQueue implements QueueInterface, ReservableQueueInterface
{
    /** @var string Name of the JSON store document inside the directory. */
    private const STORE_FILE = 'azera-queue-store.json';

    /** @var string Name of the advisory lock file inside the directory. */
    private const LOCK_FILE = 'azera-queue-store.lock';

    /**
     * Options understood by this backend. Anything else is rejected so a
     * misconfigured dispatcher fails loudly instead of silently dropping
     * settings (mirrors {@see DatabaseQueue}).
     */
    private const SUPPORTED_OPTIONS = ['queue', 'delay'];

    /**
     * @param string                          $directory  Directory to hold the store + lock file.
     * @param JobSerializerInterface          $serializer Job serializer (default NativeSerializer).
     * @param EventDispatcherInterface|null   $dispatcher Optional PSR-14 dispatcher.
     * @param int                             $reserveFor Seconds a reserved job is held before it can be re-reserved (default 60).
     */
    public function __construct(
        private string $directory,
        private JobSerializerInterface $serializer = new NativeSerializer(),
        private ?EventDispatcherInterface $dispatcher = null,
        private int $reserveFor = 60,
    ) {
        if (!is_dir($directory) && !@mkdir($directory, 0777, true) && !is_dir($directory)) {
            throw new QueueException(sprintf('FileQueue: cannot create directory "%s".', $directory));
        }
    }

    public function push(JobInterface $job, array $options = []): mixed
    {
        $unknown = array_diff(array_keys($options), self::SUPPORTED_OPTIONS);
        if ($unknown !== []) {
            throw new QueueException(sprintf(
                'FileQueue does not support option(s): %s. Supported: %s.',
                implode(', ', $unknown),
                implode(', ', self::SUPPORTED_OPTIONS),
            ));
        }

        $queue       = $options['queue'] ?? $job->queue();
        $delay       = (int) ($options['delay'] ?? 0);
        $availableAt = time() + $delay;
        $payload     = $this->serializer->serialize($job);
        $jobId       = $job->id();

        return $this->withLock(function (array $state) use ($queue, $payload, $availableAt, $jobId, $job): array {
            // Deduplication: when the job carries an id and one is already
            // queued, return the existing id instead of enqueueing twice.
            if ($jobId !== null) {
                foreach ($state['jobs'] as $existing) {
                    if (($existing['dedup_key'] ?? null) === $jobId) {
                        $this->dispatcher?->dispatch(new JobPushed($queue, (string) $existing['id'], $job::class));
                        return ['state' => $state, 'return' => $existing['id']];
                    }
                }
            }

            $id = $state['nextId'];
            $state['nextId'] = $id + 1;

            $state['jobs'][] = [
                'id'           => $id,
                'queue'        => $queue,
                'payload'      => $payload,
                'attempts'     => 0,
                'available_at' => $availableAt,
                'reserved_at'  => null,
                'created_at'   => time(),
                'dedup_key'    => $jobId,
            ];

            $this->dispatcher?->dispatch(new JobPushed($queue, (string) $id, $job::class));

            return ['state' => $state, 'return' => $id];
        });
    }

    public function registerWorker(string $jobClass, ?callable $handler = null): void
    {
        // Like DatabaseQueue, the Worker resolves handlers itself; this is
        // a no-op for parity with the interface.
    }

    /**
     * Reserve the next available job on $queue, marking it as reserved.
     *
     * Returns the raw row (id, payload, attempts) or null when no job is
     * available. The Worker deserializes the payload and runs it.
     *
     * @return array{id: string|int, payload: string, attempts: int}|null
     */
    public function reserve(string $queue = 'default'): ?array
    {
        return $this->withLock(function (array $state) use ($queue): array {
            $now       = time();
            $expiredAt = $now - $this->reserveFor;

            foreach ($state['jobs'] as $i => $job) {
                if ($job['queue'] !== $queue) {
                    continue;
                }

                $isAvailable = $job['available_at'] <= $now
                    && ($job['reserved_at'] === null || $job['reserved_at'] <= $expiredAt);

                if ($isAvailable) {
                    $state['jobs'][$i]['reserved_at'] = $now;
                    $state['jobs'][$i]['attempts']    = (int) $job['attempts'] + 1;

                    return [
                        'state'  => $state,
                        'return' => [
                            'id'       => $job['id'],
                            'payload'  => $job['payload'],
                            'attempts' => $state['jobs'][$i]['attempts'],
                        ],
                    ];
                }
            }

            return ['state' => $state, 'return' => null];
        });
    }

    /**
     * Delete a processed job by id.
     */
    public function delete(string|int $id): void
    {
        $this->withLock(function (array $state) use ($id): array {
            foreach ($state['jobs'] as $i => $job) {
                if ((int) $job['id'] === (int) $id) {
                    array_splice($state['jobs'], $i, 1);
                    break;
                }
            }
            return ['state' => $state, 'return' => null];
        });
    }

    /**
     * Release a reserved job back to the queue after $delay seconds.
     */
    public function release(string|int $id, int $delay = 0): void
    {
        $this->withLock(function (array $state) use ($id, $delay): array {
            foreach ($state['jobs'] as $i => $job) {
                if ((int) $job['id'] === (int) $id) {
                    $state['jobs'][$i]['reserved_at']  = null;
                    $state['jobs'][$i]['available_at'] = time() + $delay;
                    break;
                }
            }
            return ['state' => $state, 'return' => null];
        });
    }

    /**
     * Count all jobs on $queue (available, delayed, and reserved/in-flight).
     */
    public function count(string $queue = 'default'): int
    {
        return $this->withLock(function (array $state) use ($queue): array {
            $n = 0;
            foreach ($state['jobs'] as $job) {
                if ($job['queue'] === $queue) {
                    $n++;
                }
            }
            return ['state' => $state, 'return' => $n];
        });
    }

    /**
     * Count jobs on $queue that are immediately claimable: not delayed and
     * not reserved (or past their visibility timeout).
     */
    public function countAvailable(string $queue = 'default'): int
    {
        return $this->withLock(function (array $state) use ($queue): array {
            $now       = time();
            $expiredAt = $now - $this->reserveFor;

            $n = 0;
            foreach ($state['jobs'] as $job) {
                if ($job['queue'] !== $queue) {
                    continue;
                }
                if (
                    $job['available_at'] <= $now
                        && ($job['reserved_at'] === null || $job['reserved_at'] <= $expiredAt)
                ) {
                    $n++;
                }
            }
            return ['state' => $state, 'return' => $n];
        });
    }

    /**
     * Run $callback while holding the exclusive lock, persist any state
     * changes, and return the value the callback declares via
     * `['state' => ..., 'return' => ...]`.
     */
    private function withLock(callable $callback): mixed
    {
        $lockPath = $this->directory . DIRECTORY_SEPARATOR . self::LOCK_FILE;

        $fp = @fopen($lockPath, 'c');
        if ($fp === false) {
            throw new QueueException(sprintf('FileQueue: cannot open lock file "%s".', $lockPath));
        }

        try {
            if (!flock($fp, LOCK_EX)) {
                throw new QueueException(sprintf('FileQueue: cannot acquire lock in "%s".', $this->directory));
            }

            $state  = $this->load();
            $result = $callback($state);
            $this->store($result['state']);

            flock($fp, LOCK_UN);

            return $result['return'] ?? null;
        } finally {
            fclose($fp);
        }
    }

    /**
     * @return array{nextId: int, jobs: array<int, array<string, mixed>>}
     */
    private function load(): array
    {
        $path = $this->directory . DIRECTORY_SEPARATOR . self::STORE_FILE;

        if (!is_file($path)) {
            return ['nextId' => 1, 'jobs' => []];
        }

        $raw = @file_get_contents($path);
        if ($raw === false) {
            throw new QueueException(sprintf('FileQueue: failed to read state file "%s".', $path));
        }

        $data = json_decode($raw, true);
        if (!is_array($data)) {
            throw new QueueException(sprintf('FileQueue: corrupted state file "%s".', $path));
        }

        return [
            'nextId' => (int) ($data['nextId'] ?? 1),
            'jobs'   => is_array($data['jobs'] ?? null) ? $data['jobs'] : [],
        ];
    }

    /**
     * Write the state atomically: temp file in the same directory, then
     * rename over the target so readers never observe a partial write.
     *
     * @param array{nextId: int, jobs: array<int, array<string, mixed>>} $state
     */
    private function store(array $state): void
    {
        $path = $this->directory . DIRECTORY_SEPARATOR . self::STORE_FILE;

        $json = json_encode(
            ['nextId' => $state['nextId'], 'jobs' => array_values($state['jobs'])],
            JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES,
        );

        if ($json === false) {
            throw new QueueException('FileQueue: failed to encode state.');
        }

        $tmp = $path . '.' . uniqid('', true) . '.tmp';

        if (@file_put_contents($tmp, $json) === false) {
            throw new QueueException(sprintf('FileQueue: failed to write temporary state file "%s".', $tmp));
        }

        if (!@rename($tmp, $path)) {
            @unlink($tmp);
            throw new QueueException(sprintf('FileQueue: failed to move state file into place at "%s".', $path));
        }
    }
}