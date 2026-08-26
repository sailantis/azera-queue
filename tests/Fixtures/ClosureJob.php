<?php

declare(strict_types=1);

namespace Azera\Queue\Tests\Fixtures;

use Azera\Queue\Job;

final class ClosureJob extends Job
{
    public function __construct(
        private \Closure $fn,
    ) {}

    public function handle(): void
    {
        ($this->fn)();
    }
}