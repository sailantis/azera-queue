<?php

declare(strict_types=1);

namespace Azera\Queue\Worker;

use Azera\AppContext;
use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\Event\JobFailed;
use Azera\Queue\Event\JobProcessed;
use Azera\Queue\Event\JobReleased;
use Azera\Queue\Event\JobReserved;
use Azera\Queue\Event\JobRetried;
use Azera\Queue\FailedJob\FailedJobProviderInterface;
use Azera\Queue\QueueInterface;
use Azera\Queue\Serialization\JobSerializerInterface;
use Azera\Queue\Serialization\NativeSerializer;
use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Throwable;

/**
 * Long-running queue worker.
 *
 * Pops jobs from a {@see ReservableQueueInterface} backend, deserializes
 * them, and invokes {@see \Azera\Queue\JobInterface::handle()}. Honors
 * `tries()`, `backoff()`, and `retryUntil()` for retry control; on
 * failure, releases the job for another attempt (with backoff) or moves
 * it to the {@see FailedJobProviderInterface} when attempts are
 * exhausted.
 *
 * The worker is driven by the {@see QueueTask} CLI command but can be
 * invoked programmatically (e.g. from a daemon). Loop control options:
 *  - `once`         Run a single job and exit.
 *  - `stopWhenEmpty` Exit when the queue has no available job.
 *  - `maxJobs`      Exit after processing N jobs.
 *  - `maxSeconds`   Exit after N seconds elapsed.
 *  - `memoryLimit`  Exit (gracefully) when memory exceeds N MB.
 *  - `sleep`        Seconds to idle-poll when no job is available.
 *  - `queue`        The queue name to consume.
 */
class Worker
{
    /**
     * @param AppContext                       $context       Used to resolve the queue and optional services.
     * @param ReservableQueueInterface          $queue         The queue backend to consume.
     * @param JobSerializerInterface            $serializer    Job serializer (default NativeSerializer).
     * @param FailedJobProviderInterface|null   $failed        Optional failed-job storage.
     * @param EventDispatcherInterface|null      $dispatcher    Optional PSR-14 dispatcher.
     * @param LoggerInterface|null              $logger         Optional logger.
     * @param int                               $sleep         Idle-poll seconds (default 1).
     */
    public function __construct(
        private AppContext $context,
        private ReservableQueueInterface $queue,
        private JobSerializerInterface $serializer = new NativeSerializer(),
        private ?FailedJobProviderInterface $failed = null,
        private ?EventDispatcherInterface $dispatcher = null,
        private LoggerInterface $logger = new NullLogger(),
        private int $sleep = 1,
    ) {}

    /**
     * Run the worker loop.
     *
     * @param array{queue?: string, once?: bool, stopWhenEmpty?: bool, maxJobs?: int, maxSeconds?: int, memoryLimit?: int} $options
     * @return array{processed: int, failed: int} Counters for reporting.
     */
    public function run(array $options = []): array
    {
        $queueName     = $options['queue'] ?? 'default';
        $once          = (bool) ($options['once'] ?? false);
        $stopWhenEmpty = (bool) ($options['stopWhenEmpty'] ?? false);
        $maxJobs       = (int) ($options['maxJobs'] ?? 0);
        $maxSeconds    = (int) ($options['maxSeconds'] ?? 0);
        $memoryLimit   = (int) ($options['memoryLimit'] ?? 0);

        $processed = 0;
        $failed    = 0;
        $startedAt = time();

        while (true) {
            if ($maxJobs > 0 && $processed + $failed >= $maxJobs) {
                break;
            }
            if ($maxSeconds > 0 && (time() - $startedAt) >= $maxSeconds) {
                break;
            }
            if ($memoryLimit > 0 && $this->memoryMb() >= $memoryLimit) {
                $this->logger->info('Worker memory limit reached, exiting gracefully.', ['mb' => $this->memoryMb()]);
                break;
            }

            $reserved = $this->queue->reserve($queueName);

            if ($reserved === null) {
                if ($once || $stopWhenEmpty) {
                    break;
                }
                sleep($this->sleep);
                continue;
            }

            $processed++;

            $this->process($queueName, $reserved);

            if ($once) {
                break;
            }
        }

        return ['processed' => $processed, 'failed' => $failed];
    }

    /**
     * Process a single reserved job: deserialize, handle, and either
     * delete (success) or release/retry/fail.
     */
    private function process(string $queueName, array $reserved): void
    {
        $id       = $reserved['id'];
        $attempt  = $reserved['attempts'];
        $job      = null;
        $jobClass = null;

        try {
            $job      = $this->serializer->deserialize($reserved['payload']);
            $jobClass = $job::class;

            $this->dispatch(new JobReserved($queueName, (string) $id, $jobClass, $attempt));

            $start = microtime(true);
            $job->handle();
            $durationMs = (microtime(true) - $start) * 1000.0;

            $this->queue->delete($id);

            $this->dispatch(new JobProcessed($queueName, (string) $id, $jobClass, $attempt, $durationMs));
        } catch (Throwable $e) {
            $jobClass ??= 'unknown';
            $this->handleFailure($queueName, (string) $id, $job, $jobClass, $attempt, $e);
        }
    }

    /**
     * Decide whether to retry or fail the job based on its declared
     * retry policy, and act accordingly.
     */
    private function handleFailure(
        string $queueName,
        string $id,
        ?object $job,
        string $jobClass,
        int $attempt,
        Throwable $e,
    ): void {
        $tries      = $this->tries($job);
        $retryUntil = $this->retryUntil($job);
        $backoff    = $this->backoffSeconds($job, $attempt);

        $exhausted = $attempt >= $tries
            || ($retryUntil !== null && time() >= $retryUntil);

        if ($exhausted) {
            $this->logger->error('Job failed permanently', [
                'queue'   => $queueName,
                'id'      => $id,
                'class'   => $jobClass,
                'attempt' => $attempt,
                'error'   => $e->getMessage(),
            ]);

            $this->dispatch(new JobFailed($queueName, $id, $jobClass, $e, $attempt));

            if ($this->failed !== null && $job !== null) {
                $this->failed->log($queueName, $this->serializer->serialize($job), $e);
            }

            $this->queue->delete($id);

            if ($job !== null && method_exists($job, 'failed')) {
                $job->failed($e);
            }
            return;
        }

        $this->logger->warning('Job retrying', [
            'queue'      => $queueName,
            'id'         => $id,
            'class'      => $jobClass,
            'attempt'    => $attempt,
            'next_delay' => $backoff,
            'error'      => $e->getMessage(),
        ]);

        $this->dispatch(new JobRetried($queueName, $id, $jobClass, $e, $attempt, $backoff));

        $this->queue->release($id, $backoff);
    }

    private function tries(?object $job): int
    {
        if ($job === null) {
            return 1;
        }
        return $job instanceof \Azera\Queue\JobInterface ? $job->tries() : 1;
    }

    private function retryUntil(?object $job): ?int
    {
        if ($job === null) {
            return null;
        }
        return $job instanceof \Azera\Queue\JobInterface ? $job->retryUntil() : null;
    }

    private function backoffSeconds(?object $job, int $attempt): int
    {
        if ($job === null) {
            return 0;
        }
        if (!$job instanceof \Azera\Queue\JobInterface) {
            return 0;
        }
        $backoff = $job->backoff();
        if (is_array($backoff)) {
            $index = max(0, $attempt - 1);
            return (int) ($backoff[$index] ?? 0);
        }
        return (int) $backoff;
    }

    private function memoryMb(): int
    {
        return (int) round(memory_get_usage(true) / 1024 / 1024);
    }

    private function dispatch(object $event): void
    {
        $this->dispatcher?->dispatch($event);
    }
}