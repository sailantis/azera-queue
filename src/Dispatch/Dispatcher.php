<?php

declare(strict_types=1);

namespace Azera\Queue\Dispatch;

use Azera\AppContext;
use Azera\Queue\QueueInterface;

/**
 * Fluent job dispatcher.
 *
 * Resolves the queue backend and pushes a job with options (queue name,
 * delay, priority) in a single expressive call:
 *
 * ```php
 * Dispatcher::dispatch(new SendEmailJob('user@example.com'))
 *     ->onQueue('emails')
 *     ->delay(60)
 *     ->priority('high');
 * ```
*
 * With {@see \Azera\Queue\SyncQueue} (the default), the job runs inline.
 * With an async backend, it is serialized and a worker picks it up.
 */
class Dispatcher
{
    private string $queue = 'default';

    private int $delay = 0;

    private mixed $priority = null;

    private function __construct(
        private QueueInterface $queueBackend,
        private \Azera\Queue\JobInterface $job,
    ) {}

    /**
     * Create a dispatcher for the job, resolving the queue from the
     * application context.
     */
    public static function dispatch(\Azera\Queue\JobInterface $job): self
    {
        $queue = AppContext::instance()->get(QueueInterface::class);
        return new self($queue, $job);
    }

    /**
     * Set the target queue name.
     */
    public function onQueue(string $queue): self
    {
        $this->queue = $queue;
        return $this;
    }

    /**
     * Delay the job by $seconds.
     */
    public function delay(int $seconds): self
    {
        $this->delay = $seconds;
        return $this;
    }

    /**
     * Set the job priority (backend-specific).
     */
    public function priority(mixed $priority): self
    {
        $this->priority = $priority;
        return $this;
    }

    /**
     * Push the job with the accumulated options.
     *
     * @return mixed The job id (backend-specific) or null.
     */
    public function send(): mixed
    {
        return $this->queueBackend->push($this->job, [
            'queue'    => $this->queue,
            'delay'    => $this->delay,
            'priority' => $this->priority,
        ]);
    }
}