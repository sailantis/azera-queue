<?php

declare(strict_types=1);

namespace Azera\Queue\Tests;

use Azera\Queue\QueueException;
use Azera\Queue\Serialization\NativeSerializer;
use PHPUnit\Framework\TestCase;

final class NativeSerializerTest extends TestCase
{
    public function test_round_trip(): void
    {
        $serializer = new NativeSerializer();
        $job        = new Fixtures\StubJob('hello');

        $payload  = $serializer->serialize($job);
        $restored = $serializer->deserialize($payload);

        self::assertEquals($job, $restored);
        self::assertSame('hello', $restored->value);
    }

    public function test_rejects_closure_jobs(): void
    {
        $serializer = new NativeSerializer();
        $job        = new Fixtures\ClosureJob(fn() => 'x');

        $this->expectException(QueueException::class);
        $serializer->serialize($job);
    }

    public function test_empty_payload_throws(): void
    {
        $serializer = new NativeSerializer();

        $this->expectException(QueueException::class);
        $serializer->deserialize('');
    }

    public function test_allowlist_blocks_unauthorized_class(): void
    {
        $serializer = new NativeSerializer([Fixtures\StubJob::class]);
        $job        = new Fixtures\StubJob('a');
        $payload    = $serializer->serialize($job);

        $restored = $serializer->deserialize($payload);
        self::assertInstanceOf(Fixtures\StubJob::class, $restored);
        self::assertSame('a', $restored->value);
    }

    public function test_allowlist_rejects_unlisted_class(): void
    {
        $serializer = new NativeSerializer([]); // no classes allowed
        $payload    = serialize(new Fixtures\StubJob('a'));

        $restored = $serializer->deserialize($payload);
        self::assertNotInstanceOf(Fixtures\StubJob::class, $restored);
    }
}