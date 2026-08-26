<?php

declare(strict_types=1);

namespace Azera\Queue\Tests\Fixtures;

use Azera\Queue\Job;

/**
 * A job carrying an explicit id (dedup key), to test idempotent pushes.
 */
final class DedupJob extends Job
{
    public function __construct(public string $value, private string $dedupKey) {}

    public function handle(): void
    {
        // No-op for tests.
    }

    public function id(): ?string
    {
        return $this->dedupKey;
    }
}