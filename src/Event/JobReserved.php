<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired when a worker reserves (pops) a job for processing.
 */
final class JobReserved extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly ?string $jobClass = null,
        public readonly int $attempt = 1,
    ) {}
}