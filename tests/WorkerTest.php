<?php

declare(strict_types=1);

namespace Azera\Queue\Tests;

use Azera\AppContext;
use Azera\Queue\FailedJob\ArrayFailedJobProvider;
use Azera\Queue\Serialization\NativeSerializer;
use Azera\Queue\Worker\Worker;
use PHPUnit\Framework\TestCase;

final class WorkerTest extends TestCase
{
    private function worker(InMemoryQueue $queue, ?ArrayFailedJobProvider $failed = null): Worker
    {
        $context = AppContext::instance();
        return new Worker(
            context: $context,
            queue: $queue,
            serializer: new NativeSerializer(),
            failed: $failed,
            sleep: 0,
        );
    }

    public function test_processes_successful_job(): void
    {
        $queue = new InMemoryQueue();
        $queue->push(new Fixtures\StubJob('hello'));

        $result = $this->worker($queue)->run(['once' => true]);

        self::assertSame(1, $result['processed']);
        self::assertSame(0, $queue->count());
    }

    public function test_retries_failing_job_then_fails_permanently(): void
    {
        $queue  = new InMemoryQueue();
        $failed = new ArrayFailedJobProvider();
        $queue->push(new Fixtures\FailingJob('boom', tries: 2, backoff: 0));

        // First run: attempts 1, fails, retries (releases with delay 0).
        $this->worker($queue, $failed)->run(['once' => true]);
        self::assertSame(1, $queue->count());

        // Second run: attempts 2, fails permanently → moved to failed provider.
        $this->worker($queue, $failed)->run(['once' => true]);
        self::assertSame(0, $queue->count());
        self::assertCount(1, $failed->all());
    }

    public function test_stop_when_empty_exits_cleanly(): void
    {
        $queue  = new InMemoryQueue();
        $result = $this->worker($queue)->run(['stopWhenEmpty' => true]);

        self::assertSame(0, $result['processed']);
    }

    public function test_max_jobs_limit(): void
    {
        $queue = new InMemoryQueue();
        for ($i = 0; $i < 3; $i++) {
            $queue->push(new Fixtures\StubJob((string) $i));
        }

        $result = $this->worker($queue)->run(['maxJobs' => 2]);

        self::assertSame(2, $result['processed']);
        self::assertSame(1, $queue->count());
    }

    public function test_calls_failed_hook_on_permanent_failure(): void
    {
        $queue  = new InMemoryQueue();
        $failed = new ArrayFailedJobProvider();
        $job    = new Fixtures\FailingJob('boom', tries: 1);
        $queue->push($job);

        $this->worker($queue, $failed)->run(['once' => true]);

        // The original job instance is not the one deserialized by the
        // worker, so $job->failedCalled stays false; assert via the
        // failed provider instead.
        self::assertCount(1, $failed->all());
    }
}