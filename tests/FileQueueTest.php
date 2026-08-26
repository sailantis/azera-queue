<?php

declare(strict_types=1);

namespace Azera\Queue\Tests;

use Azera\AppContext;
use Azera\Queue\Backend\FileQueue;
use Azera\Queue\QueueException;
use Azera\Queue\Serialization\NativeSerializer;
use Azera\Queue\Worker\Worker;
use PHPUnit\Framework\TestCase;

final class FileQueueTest extends TestCase
{
    private string $dir;

    protected function setUp(): void
    {
        $this->dir = sys_get_temp_dir() . '/azera-queue-' . bin2hex(random_bytes(6));
    }

    protected function tearDown(): void
    {
        foreach (glob($this->dir . '/*') ?: [] as $file) {
            @unlink($file);
        }
        @rmdir($this->dir);
    }

    private function queue(int $reserveFor = 60): FileQueue
    {
        return new FileQueue($this->dir, reserveFor: $reserveFor);
    }

    private function worker(FileQueue $queue): Worker
    {
        return new Worker(
            context: AppContext::instance(),
            queue: $queue,
            serializer: new NativeSerializer(),
            sleep: 0,
        );
    }

    public function test_persists_across_instances(): void
    {
        $a = $this->queue();
        $a->push(new Fixtures\StubJob('hello'));

        // A new FileQueue over the same directory sees the queued job.
        $b = $this->queue();
        self::assertSame(1, $b->count());
        $result = $this->worker($b)->run(['once' => true]);
        self::assertSame(1, $result['processed']);
        self::assertSame(0, $b->count());
    }

    public function test_processes_successful_job(): void
    {
        $queue = $this->queue();
        $queue->push(new Fixtures\StubJob('hello'));

        $result = $this->worker($queue)->run(['once' => true]);

        self::assertSame(1, $result['processed']);
        self::assertSame(0, $queue->count());
    }

    public function test_retries_failing_job_then_fails_permanently(): void
    {
        $queue = $this->queue();
        $queue->push(new Fixtures\FailingJob('boom', tries: 2, backoff: 0));

        // First run: attempt 1, fails, retries (released with delay 0).
        $this->worker($queue)->run(['once' => true]);
        self::assertSame(1, $queue->count());

        // Second run: attempt 2, fails permanently → job removed.
        $this->worker($queue)->run(['once' => true]);
        self::assertSame(0, $queue->count());
    }

    public function test_reserve_honors_visibility_timeout(): void
    {
        $queue = $this->queue(reserveFor: 10);

        $queue->push(new Fixtures\StubJob('job'));

        $reserved = $queue->reserve();
        self::assertNotNull($reserved);

        // Still reserved: count includes it, but countAvailable does not.
        self::assertSame(1, $queue->count());
        self::assertSame(0, $queue->countAvailable());

        // Release makes it claimable again immediately (delay 0).
        $queue->release($reserved['id']);
        self::assertSame(1, $queue->countAvailable());
    }

    public function test_release_with_delay_defers_availability(): void
    {
        $queue = $this->queue();
        $queue->push(new Fixtures\StubJob('job'));

        $reserved = $queue->reserve();
        self::assertNotNull($reserved);

        $queue->release($reserved['id'], delay: 30);
        self::assertSame(0, $queue->countAvailable());
    }

    public function test_stale_reserved_job_is_reclaimable_after_timeout(): void
    {
        // A 1-second visibility timeout so the reservation expires fast.
        $queue = $this->queue(reserveFor: 1);
        $queue->push(new Fixtures\StubJob('stale'));

        self::assertNotNull($queue->reserve());
        self::assertSame(0, $queue->countAvailable());

        sleep(2);

        // Past the timeout the job is claimable again.
        self::assertSame(1, $queue->countAvailable());
        self::assertNotNull($queue->reserve());
    }

    public function test_delete_removes_job(): void
    {
        $queue = $this->queue();
        $id    = $queue->push(new Fixtures\StubJob('job'));
        self::assertSame(1, $queue->count());

        $queue->delete($id);
        self::assertSame(0, $queue->count());
    }

    public function test_count_is_per_queue(): void
    {
        $queue = $this->queue();
        $queue->push(new Fixtures\StubJob('a'), ['queue' => 'default']);
        $queue->push(new Fixtures\StubJob('b'), ['queue' => 'high']);

        self::assertSame(1, $queue->count('default'));
        self::assertSame(1, $queue->count('high'));
        self::assertSame(0, $queue->count('nope'));
    }

    public function test_unknown_option_is_rejected(): void
    {
        $this->expectException(QueueException::class);
        $this->queue()->push(new Fixtures\StubJob('job'), ['priority' => 5]);
    }

    public function test_dedup_returns_existing_id(): void
    {
        $queue = $this->queue();

        $job = new Fixtures\DedupJob('job', 'dedup-key-42');

        $first  = $queue->push($job);
        $second = $queue->push($job);

        // Same id → second push is idempotent and returns the existing id.
        self::assertSame($first, $second);
        self::assertSame(1, $queue->count());
    }

    public function test_worker_stop_when_empty(): void
    {
        $queue  = $this->queue();
        $result = $this->worker($queue)->run(['stopWhenEmpty' => true]);
        self::assertSame(0, $result['processed']);
    }
}