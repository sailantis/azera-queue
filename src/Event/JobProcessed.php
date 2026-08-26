<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired after a job's {@see \Azera\Queue\JobInterface::handle()} completes
 * without throwing.
 */
final class JobProcessed extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly ?string $jobClass = null,
        public readonly int $attempt = 1,
        public readonly float $durationMs = 0.0,
    ) {}
}