<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Fired when a job is pushed onto a queue backend.
 */
final class JobPushed extends QueueEvent
{
    public function __construct(
        public readonly string $queue,
        public readonly ?string $jobId,
        public readonly ?string $jobClass = null,
    ) {}
}