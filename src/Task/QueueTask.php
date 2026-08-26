<?php

declare(strict_types=1);

namespace Azera\Queue\Task;

use Azera\AppContext;
use Azera\Cli\Task;
use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\FailedJob\FailedJobProviderInterface;
use Azera\Queue\QueueInterface;
use Azera\Queue\Worker\Worker;

/**
 * CLI task for the queue subsystem.
 *
 * Available actions (auto-discovered by the framework's Console):
 *
 *  - `azera queue:work`         Start the worker loop.
 *    Options: --queue=, --once, --stop-when-empty, --max-jobs=, --max-time=, --memory=, --sleep=
 *  - `azera queue:failed`       List failed jobs (requires a failed-job provider).
 *  - `azera queue:retry <id>`    Re-push a failed job by id.
 *  - `azera queue:flush`         Delete all failed jobs.
 */
class QueueTask extends Task
{
    /**
     * Process jobs from the queue.
     */
    public function workAction(): void
    {
        $ctx   = AppContext::instance();
        $queue = $ctx->get(QueueInterface::class);

        if (!$queue instanceof ReservableQueueInterface) {
            $this->stderrln('The registered queue backend does not support reservation/worker processing.');
            $this->stderrln('Use a backend that implements Azera\\Queue\\Backend\\Contract\\ReservableQueueInterface (e.g. DatabaseQueue, RedisQueue).');
            return;
        }

        $failed = $ctx->get(FailedJobProviderInterface::class);
        $worker = new Worker(
            context: $ctx,
            queue: $queue,
            failed: $failed,
        );

        $options = [
            'queue'         => $this->options['queue'] ?? 'default',
            'once'          => isset($this->options['once']),
            'stopWhenEmpty' => isset($this->options['stop-when-empty']),
            'maxJobs'       => (int) ($this->options['max-jobs'] ?? 0),
            'maxSeconds'    => (int) ($this->options['max-time'] ?? 0),
            'memoryLimit'   => (int) ($this->options['memory'] ?? 0),
        ];
        $worker->run($options);
    }

    /**
     * List failed jobs.
     */
    public function failedAction(): void
    {
        $ctx    = AppContext::instance();
        $failed = $ctx->get(FailedJobProviderInterface::class);

        if ($failed === null) {
            $this->stderrln('No failed-job provider is registered.');
            return;
        }

        $jobs = $failed->all();
        if (empty($jobs)) {
            $this->line('No failed jobs.');
            return;
        }

        foreach ($jobs as $job) {
            $this->line(sprintf('[%s] queue=%s failed_at=%s', $job['id'], $job['queue'], date('c', $job['failed_at'])));
            $this->line('  ' . $job['exception']);
        }
    }

    /**
     * Retry a failed job by id.
     */
    public function retryAction(): void
    {
        $ctx    = AppContext::instance();
        $failed = $ctx->get(FailedJobProviderInterface::class);

        if ($failed === null) {
            $this->stderrln('No failed-job provider is registered.');
            return;
        }

        $id = $this->options['id'] ?? $this->console->args[0] ?? null;
        if ($id === null) {
            $this->stderrln('Usage: azera queue:retry <id>');
            return;
        }

        $job = $failed->find($id);
        if ($job === null) {
            $this->stderrln(sprintf('No failed job with id %s.', $id));
            return;
        }

        $queue = $ctx->get(QueueInterface::class);
        $queue->push($job['payload']); // Re-push the payload string; the worker will deserialize it.

        $failed->forget($id);
        $this->info(sprintf('Failed job %s re-queued.', $id));
    }

    /**
     * Delete all failed jobs.
     */
    public function flushAction(): void
    {
        $ctx    = AppContext::instance();
        $failed = $ctx->get(FailedJobProviderInterface::class);

        if ($failed === null) {
            $this->stderrln('No failed-job provider is registered.');
            return;
        }

        $failed->flush();
        $this->info('All failed jobs deleted.');
    }
}