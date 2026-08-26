<?php

declare(strict_types=1);

namespace Azera\Queue\Tests;

use Azera\Queue\FailedJob\ArrayFailedJobProvider;
use PHPUnit\Framework\TestCase;

final class ArrayFailedJobProviderTest extends TestCase
{
    public function test_log_and_find_and_forget(): void
    {
        $provider = new ArrayFailedJobProvider();

        $id = $provider->log('default', 'payload', new \RuntimeException('boom'));

        self::assertNotNull($provider->find($id));
        self::assertCount(1, $provider->all());

        $provider->forget($id);
        self::assertNull($provider->find($id));
        self::assertCount(0, $provider->all());
    }

    public function test_flush(): void
    {
        $provider = new ArrayFailedJobProvider();
        $provider->log('q', 'p', new \RuntimeException('x'));
        $provider->log('q', 'p2', new \RuntimeException('y'));

        $provider->flush();
        self::assertCount(0, $provider->all());
    }

    public function test_all_newest_first(): void
    {
        $provider = new ArrayFailedJobProvider();
        $first    = $provider->log('q', 'p', new \RuntimeException('x'));
        $second   = $provider->log('q', 'p2', new \RuntimeException('y'));

        $all = $provider->all();
        self::assertSame($second, $all[0]['id']);
        self::assertSame($first, $all[1]['id']);
    }
}