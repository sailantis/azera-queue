<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired when a worker releases a reserved job back to the queue
 * (e.g. after a retry delay).
 */
final class JobReleased extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly int $availableAt,
    ) {}
}