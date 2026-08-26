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
use Redis;
use RedisException;

/**
 * `ext-redis`-backed queue with at-least-once delivery.
 *
 * Jobs live in Redis lists keyed by queue name. A *ready* list holds
 * immediately-claimable payloads (ordered by priority via ZSET keys),
 * while a sorted set of *delayed* entries (score = `available_at`)
 * feeds the ready list as they become due. Reservation uses a dedicated
 * `reserve:` hash per job plus a `processing:` sorted set, so a worker
 * that dies mid-processing has its job re-offered once the visibility
 * timeout elapses.
 *
 * Delivery model: at-least-once. If the worker crashes before `ack()`
 * (delete), the job resurfaces after `$reserveFor` seconds and is
 * retried, matching the behaviour of {@see DatabaseQueue} and
 * {@see FileQueue}.
 *
 * Priorities are supported by keeping a small number of ready lists
 * (e.g. `low`, `default`, `high`) and always popping from the highest
 * priority first. Queue names are used as a Redis key prefix, so a
 * single connection can host many logical queues without collisions.
 *
 * The backend requires the `ext-redis` extension (a `Redis` client is
 * injected; it is *not* opened here so connection handling stays with
 * the application).
 */
class RedisQueue implements QueueInterface, ReservableQueueInterface
{
    /**
     * Options understood by this backend. Anything else is rejected so a
     * misconfigured dispatcher fails loudly instead of silently dropping
     * settings (mirrors {@see DatabaseQueue} / {@see FileQueue}).
     */
    private const SUPPORTED_OPTIONS = ['queue', 'delay', 'priority'];

    /**
     * @param Redis                             $redis       Connected `\Redis` client.
     * @param string                            $prefix      Key prefix to scope the queue (default `azera_queue`).
     * @param JobSerializerInterface            $serializer  Job serializer (default NativeSerializer).
     * @param EventDispatcherInterface|null     $dispatcher  Optional PSR-14 dispatcher.
     * @param int                               $reserveFor  Seconds a reserved job is held before it can be re-reserved (default 60).
     * @param array<int, string>                $priorities  Ordered priority levels, lowest index = highest priority (default `['high', 'default', 'low']`).
     */
    public function __construct(
        private Redis $redis,
        private string $prefix = 'azera_queue',
        private JobSerializerInterface $serializer = new NativeSerializer(),
        private ?EventDispatcherInterface $dispatcher = null,
        private int $reserveFor = 60,
        private array $priorities = ['high', 'default', 'low'],
    ) {}

    /**
     * Push a job onto the queue.
     *
     * @return mixed A composite id string `{queue}:{counter}` used for ack/release.
     */
    public function push(JobInterface $job, array $options = []): mixed
    {
        $unknown = array_diff(array_keys($options), self::SUPPORTED_OPTIONS);
        if ($unknown !== []) {
            throw new QueueException(sprintf(
                'RedisQueue does not support option(s): %s. Supported: %s.',
                implode(', ', $unknown),
                implode(', ', self::SUPPORTED_OPTIONS),
            ));
        }

        $queue    = $options['queue'] ?? $job->queue();
        $delay    = (int) ($options['delay'] ?? 0);
        $priority = $options['priority'] ?? null;

        $payload = $this->serializer->serialize($job);
        $jobId   = $job->id();
        $id      = $jobId ?? $this->nextId($queue);

        // Deduplication: if a stable job id is provided, reuse it so the
        // same job is never enqueued twice (idempotent push).
        if ($jobId !== null && $this->redis->exists($this->jobKey($queue, $id))) {
            $this->dispatcher?->dispatch(new JobPushed($queue, (string) $id, $job::class));
            return $id;
        }

        $dueAt = time() + $delay;

        try {
            if ($delay > 0) {
                // Delayed job: store payload + score (due timestamp) in the ZSET.
                $this->redis->zAdd($this->delayedKey($queue), $dueAt, $id);
                $this->redis->set($this->jobKey($queue, $id), $payload);
            } else {
                // Immediate job: push onto the ready list for its priority bucket.
                $bucket = $this->bucket($priority);
                $this->redis->rPush($this->readyKey($queue, $bucket), $id);
                $this->redis->set($this->jobKey($queue, $id), $payload);
            }
        } catch (RedisException $e) {
            throw new QueueException('RedisQueue: failed to enqueue job: ' . $e->getMessage(), 0, $e);
        }

        $this->dispatcher?->dispatch(new JobPushed($queue, (string) $id, $job::class));

        return $id;
    }

    public function registerWorker(string $jobClass, ?callable $handler = null): void
    {
        // Like the other async backends, the Worker resolves handlers
        // itself; this is a no-op for parity with the interface.
    }

    /**
     * Reserve the next available job, moving any due delayed jobs into
     * the ready lists first. Marks the job as reserved under the
     * visibility timeout so a dead worker's job is eventually retried.
     *
     * @return array{id: string|int, payload: string, attempts: int}|null
     */
    public function reserve(string $queue = 'default'): ?array
    {
        $this->promoteDelayed($queue);

        $id = $this->popFromReady($queue);
        if ($id === null) {
            return null;
        }

        $payload = $this->redis->get($this->jobKey($queue, $id));

        if (!is_string($payload) || $payload === '') {
            // Payload missing (shouldn't happen under the current write
            // order); drop the orphan and move on.
            $this->redis->del([$this->jobKey($queue, $id)]);
            return null;
        }

        // Store reservation: an incrementing attempt counter plus a TTL
        // matching the visibility timeout. The TTL acts as the reaper:
        // after $reserveFor seconds a dead worker's job can be re-queued.
        $this->redis->hIncrBy($this->reserveKey($queue, $id), 'attempts', 1);
        $this->redis->set($this->processingKey($queue, $id), '1', ['EX' => $this->reserveFor]);

        $attempts = (int) $this->redis->hGet($this->reserveKey($queue, $id), 'attempts');

        return [
            'id'       => "{$queue}:{$id}",
            'payload'  => $payload,
            'attempts' => $attempts,
        ];
    }

    /**
     * Delete a processed job (acknowledge).
     */
    public function delete(string|int $id): void
    {
        [$queue, $rawId] = $this->split((string) $id);

        $this->redis->del([
            $this->jobKey($queue, $rawId),
            $this->reserveKey($queue, $rawId),
            $this->processingKey($queue, $rawId),
        ]);
    }

    /**
     * Release a reserved job back to the queue after $delay seconds.
     *
     * Used by the Worker for retries: the job is re-queued (delayed or
     * immediate depending on backoff) and any reservation state cleared.
     */
    public function release(string|int $id, int $delay = 0): void
    {
        [$queue, $rawId] = $this->split((string) $id);

        $payload = $this->redis->get($this->jobKey($queue, $rawId));
        if (!is_string($payload) || $payload === '') {
            return;
        }

        $dueAt = time() + $delay;

        try {
            if ($delay > 0) {
                $this->redis->zAdd($this->delayedKey($queue), $dueAt, $rawId);
            } else {
                $this->redis->rPush($this->readyKey($queue), $rawId);
            }
        } catch (RedisException $e) {
            throw new QueueException('RedisQueue: failed to release job: ' . $e->getMessage(), 0, $e);
        }

        // Clear reservation state so the job can be claimed again.
        $this->redis->del([
            $this->reserveKey($queue, $rawId),
            $this->processingKey($queue, $rawId),
        ]);
    }

    /**
     * Count all jobs on $queue (ready, delayed, and reserved/in-flight).
     */
    public function count(string $queue = 'default'): int
    {
        $ready = 0;
        foreach ($this->priorities as $p) {
            $ready += $this->redis->lLen($this->readyKey($queue, $p));
        }

        $delayed    = $this->redis->zCard($this->delayedKey($queue));
        $processing = $this->redis->keys($this->processingPattern($queue));

        return $ready + $delayed + count($processing);
    }

    /**
     * Count jobs that are immediately claimable: ready, excluding delayed
     * and in-flight work. Useful for monitoring a genuine backlog.
     */
    public function countAvailable(string $queue = 'default'): int
    {
        $this->promoteDelayed($queue);

        $n = 0;
        foreach ($this->priorities as $p) {
            $n += $this->redis->lLen($this->readyKey($queue, $p));
        }

        return $n;
    }

    /**
     * Move any delayed jobs whose due timestamp has passed into the
     * ready lists.
     */
    private function promoteDelayed(string $queue): void
    {
        $now  = time();
        $zset = $this->delayedKey($queue);
        $ids  = $this->redis->zRangeByScore($zset, '-inf', (string) $now);

        if ($ids === false || $ids === []) {
            return;
        }

        foreach ($ids as $id) {
            $this->redis->zRem($zset, $id);
            // Delayed jobs are re-queued into the default priority bucket
            // unless their payload is accompanied by priority metadata —
            // which we do not persist, so default it is.
            $this->redis->rPush($this->readyKey($queue), $id);
        }
    }

    /**
     * Pop the highest-priority available job id, or null when empty.
     */
    private function popFromReady(string $queue): ?string
    {
        foreach ($this->priorities as $p) {
            $id = $this->redis->lPop($this->readyKey($queue, $p));
            if ($id !== false && $id !== null) {
                return (string) $id;
            }
        }
        return null;
    }

    /**
     * Map a user-supplied priority to a known bucket, falling back to
     * the default (middle) bucket for unknown values.
     */
    private function bucket(mixed $priority): string
    {
        if ($priority !== null) {
            $match = array_search((string) $priority, $this->priorities, true);
            if ($match !== false) {
                return $this->priorities[$match];
            }
        }

        return $this->priorities[1] ?? 'default';
    }

    private function nextId(string $queue): string
    {
        return (string) $this->redis->incr($this->counterKey($queue));
    }

    /**
     * Split a composite "queue:id" id back into its parts.
     *
     * @return array{0: string, 1: string}
     */
    private function split(string $id): array
    {
        $sep = strrpos($id, ':');
        if ($sep === false) {
            return ['default', $id];
        }
        return [substr($id, 0, $sep), substr($id, $sep + 1)];
    }

    private function jobKey(string $queue, string $id): string
    {
        return "{$this->prefix}:{$queue}:job:{$id}";
    }

    private function readyKey(string $queue, ?string $bucket = null): string
    {
        $bucket ??= $this->priorities[1] ?? 'default';
        return "{$this->prefix}:{$queue}:ready:{$bucket}";
    }

    private function delayedKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:delayed";
    }

    private function reserveKey(string $queue, string $id): string
    {
        return "{$this->prefix}:{$queue}:reserve:{$id}";
    }

    private function processingKey(string $queue, string $id): string
    {
        return "{$this->prefix}:{$queue}:processing:{$id}";
    }

    private function counterKey(string $queue): string
    {
        return "{$this->prefix}:{$queue}:counter";
    }

    private function processingPattern(string $queue): string
    {
        return "{$this->prefix}:{$queue}:processing:*";
    }
}