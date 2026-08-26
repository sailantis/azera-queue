<?php

declare(strict_types=1);

namespace Azera\Queue\Backend;

use Azera\Queue\Backend\Amqp\AmqpTransport;
use Azera\Queue\Backend\Amqp\ExtAmqpTransport;
use Azera\Queue\Backend\Amqp\PhpAmqpLibTransport;
use Azera\Queue\Backend\Contract\ReservableQueueInterface;
use Azera\Queue\Event\JobPushed;
use Azera\Queue\JobInterface;
use Azera\Queue\QueueException;
use Azera\Queue\QueueInterface;
use Azera\Queue\Serialization\JobSerializerInterface;
use Azera\Queue\Serialization\NativeSerializer;
use Psr\EventDispatcher\EventDispatcherInterface;

/**
 * RabbitMQ-backed queue that works with either PHP AMQP implementation.
 *
 * It delegates to an {@see AmqpTransport} adapter, so it runs on:
 *
 *  - `ext-amqp` (PECL extension; global classes, `extension=amqp` in php.ini), or
 *  - `php-amqplib/php-amqplib` (pure-PHP library; namespaced classes).
 *
 * Use {@see self::fromConnection()} to build the right transport from an
 * arbitrary connection object, or pass an {@see AmqpTransport} directly.
 *
 * Delivery is at-least-once: a worker that takes a message must `ack()`
 * it; if it crashes (or the connection drops) before acknowledging,
 * RabbitMQ re-delivers the message — matching {@see DatabaseQueue},
 * {@see RedisQueue}, and {@see FileQueue}.
 *
 * Delayed jobs use the `rabbitmq-delayed-message-exchange` plugin
 * (`x-delayed-message` exchange type). When the broker lacks the plugin,
 * the transport falls back to a classic direct exchange and ignores the
 * delay (a documented limitation).
 *
 * Priorities are expressed via the AMQP `priority` header. Exchange and
 * per-consumer queues are declared lazily on first use.
 */
class AmqpQueue implements QueueInterface, ReservableQueueInterface
{
    /**
     * Options understood by this backend. Anything else is rejected so a
     * misconfigured dispatcher fails loudly instead of silently dropping
     * settings (mirrors the other backends).
     */
    private const SUPPORTED_OPTIONS = ['queue', 'delay', 'priority'];

    /** @var string Name of the exchange used for all messages. */
    private const EXCHANGE = 'azera_queue';

    /**
     * Build an AmqpQueue, auto-detecting the installed AMQP implementation.
     *
     * @param AMQPConnection|AbstractConnection $connection  Connected broker connection.
     * @param JobSerializerInterface            $serializer  Job serializer.
     * @param EventDispatcherInterface|null     $dispatcher  Optional PSR-14 dispatcher.
     * @param int                               $prefetch    Worker prefetch count.
     * @param int                               $maxPriority Highest AMQP priority to declare.
     *
     * @throws QueueException When neither `ext-amqp` nor `php-amqplib` is available.
     */
    public static function wire(
        $connection,
        JobSerializerInterface $serializer = new NativeSerializer(),
        ?EventDispatcherInterface $dispatcher = null,
        int $prefetch = 1,
        int $maxPriority = 9,
    ): self {
        return new self(self::detectTransport($connection, $prefetch), $serializer, $dispatcher, $maxPriority);
    }

    /**
     * @param AmqpTransport                     $transport   Resolved transport adapter.
     * @param JobSerializerInterface            $serializer  Job serializer.
     * @param EventDispatcherInterface|null     $dispatcher  Optional PSR-14 dispatcher.
     * @param int                               $maxPriority Highest AMQP priority to declare.
     */
    public function __construct(
        private AmqpTransport $transport,
        private JobSerializerInterface $serializer = new NativeSerializer(),
        private ?EventDispatcherInterface $dispatcher = null,
        private int $maxPriority = 9,
    ) {}

    /**
     * Push a job onto the queue.
     *
     * @return mixed A message id when one can be derived, otherwise null
     *   (AMQP has no native id; dedup relies on the job's `id()`).
     */
    public function push(JobInterface $job, array $options = []): mixed
    {
        $unknown = array_diff(array_keys($options), self::SUPPORTED_OPTIONS);
        if ($unknown !== []) {
            throw new QueueException(sprintf(
                'AmqpQueue does not support option(s): %s. Supported: %s.',
                implode(', ', $unknown),
                implode(', ', self::SUPPORTED_OPTIONS),
            ));
        }

        $queue    = $options['queue'] ?? $job->queue();
        $delay    = (int) ($options['delay'] ?? 0);
        $priority = $options['priority'] ?? null;

        $messageId = $job->id() ?? bin2hex(random_bytes(16));

        $this->transport->publish(
            self::EXCHANGE,
            $queue,
            $this->serializer->serialize($job),
            $this->normalizePriority($priority),
            $delay > 0 ? $delay * 1000 : null,
            $messageId,
        );

        $this->dispatcher?->dispatch(new JobPushed($queue, $messageId, $job::class));

        return $messageId;
    }

    public function registerWorker(string $jobClass, ?callable $handler = null): void
    {
        // Like the other async backends, the Worker resolves handlers
        // itself; this is a no-op for parity with the interface.
    }

    /**
     * Reserve the next available job on $queue.
     *
     * Uses a non-blocking fetch with manual ack. The Worker calls
     * {@see delete()} (ack) on success or {@see release()} (nack) on retry.
     *
     * @return array{id: string|int, payload: string, attempts: int}|null
     */
    public function reserve(string $queue = 'default'): ?array
    {
        $message = $this->transport->get($queue);
        if ($message === null) {
            return null;
        }

        $messageId = $message['messageId'];

        $this->pending[$queue][$messageId] = [
            'tag'  => $message['tag'],
            'body' => $message['body'],
        ];

        return [
            'id'       => "{$queue}:{$messageId}",
            'payload'  => $message['body'],
            'attempts' => $message['redelivered'] ? 2 : 1,
        ];
    }

    /**
     * Acknowledge (ack) a processed job.
     */
    public function delete(string|int $id): void
    {
        [$queue, $rawId] = $this->split((string) $id);

        $record = $this->pending[$queue][$rawId] ?? null;
        if ($record === null) {
            return;
        }

        $this->transport->ack($queue, $record['tag']);
        unset($this->pending[$queue][$rawId]);
    }

    /**
     * Release a reserved job back to the queue after $delay seconds.
     *
     * When $delay is 0 the message is requeued immediately. When $delay >
     * 0 the message is nacked (no requeue) and re-published to the delayed
     * exchange with an `x-delay` header so RabbitMQ re-offers it after the
     * backoff window.
     */
    public function release(string|int $id, int $delay = 0): void
    {
        [$queue, $rawId] = $this->split((string) $id);

        $record = $this->pending[$queue][$rawId] ?? null;
        if ($record === null) {
            return;
        }

        if ($delay > 0) {
            $this->transport->nack($queue, $record['tag'], false);
            $this->transport->publish(
                self::EXCHANGE,
                $queue,
                $record['body'],
                $this->maxPriority,
                $delay * 1000,
                $rawId,
            );
        } else {
            $this->transport->nack($queue, $record['tag'], true);
        }

        unset($this->pending[$queue][$rawId]);
    }

    /**
     * Count messages on $queue.
     */
    public function count(string $queue = 'default'): int
    {
        return $this->transport->count($queue);
    }

    /**
     * Count messages immediately claimable (ready) on $queue.
     */
    public function countAvailable(string $queue = 'default'): int
    {
        return $this->transport->count($queue);
    }

    private function normalizePriority(mixed $priority): int
    {
        if ($priority === null) {
            return 0;
        }
        return min(max((int) $priority, 0), 9);
    }

    /**
     * Split a composite "queue:id" id into its parts.
     *
     * @return array{0: string, 1: string}
     */
    private function split(string $id): array
    {
        $sep = strrpos($id, ':');
        if ($sep === false) {
            return ['default', $id];
        }
        return [substr($id, 0, $sep), substr($id, $sep + 1)];
    }

    /**
     * Pick the transport adapter for the given connection object.
     *
     * @param mixed $connection
     */
    private static function detectTransport($connection, int $prefetch): AmqpTransport
    {
        if ($connection instanceof \AMQPConnection) {
            return new ExtAmqpTransport($connection, $prefetch);
        }

        if ($connection instanceof \PhpAmqpLib\Connection\AbstractConnection) {
            return new PhpAmqpLibTransport($connection, $prefetch);
        }

        throw new QueueException(sprintf(
            'AmqpQueue: unsupported connection type "%s". Provide an ext-amqp AMQPConnection or a php-amqplib AbstractConnection.',
            get_debug_type($connection),
        ));
    }

    /**
     * In-flight bookkeeping: delivery tags + bodies keyed by queue:id so
     * delete()/release() can ack/nack the right message.
     *
     * @var array<string, array<string, array{tag: string, body: string}>>
     */
    private array $pending = [];
}