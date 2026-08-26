<?php

declare(strict_types=1);

namespace Azera\Queue\Tests\Fixtures;

use Azera\Queue\Job;

final class FailingJob extends Job
{
    public function __construct(
        public string $reason = 'boom',
        private int $tries = 2,
        private int $backoff = 0,
        public bool $failedCalled = false,
    ) {}

    public function handle(): void
    {
        throw new \RuntimeException($this->reason);
    }

    public function tries(): int
    {
        return $this->tries;
    }

    public function backoff(): int|array
    {
        return $this->backoff;
    }

    public function failed(\Throwable $exception): void
    {
        $this->failedCalled = true;
    }
}