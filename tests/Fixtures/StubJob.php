<?php

declare(strict_types=1);

namespace Azera\Queue\Tests\Fixtures;

use Azera\Queue\Job;

final class StubJob extends Job
{
    public function __construct(
        public string $value,
        private int $tries = 1,
        private int $backoff = 0,
    ) {}

    public function handle(): void
    {
        // No-op for tests; assertions inspect state instead.
    }

    public function tries(): int
    {
        return $this->tries;
    }

    public function backoff(): int|array
    {
        return $this->backoff;
    }
}