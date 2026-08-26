<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired when a job is retried after a failure (attempts remain).
 */
final class JobRetried extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly ?string $jobClass,
        public readonly \Throwable $exception,
        public readonly int $attempt,
        public readonly int $delaySeconds,
    ) {}
}