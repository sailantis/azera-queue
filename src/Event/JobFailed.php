<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired when a job throws an exception and no further retries remain
 * (or the worker gives up). The job is then moved to the failed-job
 * provider if one is configured.
 */
final class JobFailed extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly ?string $jobClass,
        public readonly \Throwable $exception,
        public readonly int $attempt,
    ) {}
}