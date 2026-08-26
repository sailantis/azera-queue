<?php

declare(strict_types=1);

namespace Azera\Queue\Backend\Amqp;

use AMQPChannel;
use AMQPConnection;
use AMQPExchange;
use AMQPQueue as AMQPBrokerQueue;
use Azera\Queue\QueueException;
use Throwable;

/**
 * AMQP transport backed by the `ext-amqp` PECL extension.
 *
 * Requires `extension=amqp` in php.ini. Classes are global (no namespace).
 */
final class ExtAmqpTransport implements AmqpTransport
{
    private ?AMQPChannel $channel = null;

    /** @var array<string, AMQPExchange> */
    private array $exchanges = [];

    /** @var array<string, AMQPBrokerQueue> */
    private array $queues = [];

    public function __construct(
        private AMQPConnection $connection,
        private int $prefetch = 1,
    ) {}

    public function publish(
        string $exchange,
        string $routingKey,
        string $body,
        int $priority,
        ?int $delayMs,
        string $messageId,
    ): void {
        $attributes = [
            'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT,
            'priority'      => $priority,
            'message_id'    => $messageId,
        ];

        if ($delayMs !== null && $delayMs > 0) {
            // The delayed-message exchange honours an `x-delay` header (ms).
            $attributes['headers'] = ['x-delay' => $delayMs];
        }

        try {
            $this->exchange($exchange)->publish($body, $routingKey, null, $attributes);
        } catch (Throwable $e) {
            throw new QueueException('ext-amqp: failed to publish: ' . $e->getMessage(), 0, $e);
        }
    }

    public function get(string $queue): ?array
    {
        try {
            $envelope = $this->brokerQueue($queue)->get();
        } catch (Throwable $e) {
            throw new QueueException('ext-amqp: failed to get message: ' . $e->getMessage(), 0, $e);
        }

        if ($envelope === null) {
            return null;
        }

        return [
            'body'        => $envelope->getBody(),
            'tag'         => (string) $envelope->getDeliveryTag(),
            'messageId'   => $envelope->getMessageId() ?? (string) $envelope->getDeliveryTag(),
            'redelivered' => $envelope->isRedelivery(),
        ];
    }

    public function ack(string $queue, string $tag): void
    {
        try {
            $this->brokerQueue($queue)->ack((int) $tag);
        } catch (Throwable $e) {
            throw new QueueException('ext-amqp: failed to ack: ' . $e->getMessage(), 0, $e);
        }
    }

    public function nack(string $queue, string $tag, bool $requeue): void
    {
        try {
            $this->brokerQueue($queue)->nack((int) $tag, $requeue ? AMQP_REQUEUE : 0);
        } catch (Throwable $e) {
            throw new QueueException('ext-amqp: failed to nack: ' . $e->getMessage(), 0, $e);
        }
    }

    public function declareExchange(string $name): void
    {
        $this->exchange($name);
    }

    public function declareQueue(string $queue, string $exchange, int $maxPriority): void
    {
        $this->brokerQueue($queue, $exchange, $maxPriority);
    }

    public function count(string $queue): int
    {
        try {
            return (int) $this->brokerQueue($queue)->declareQueue();
        } catch (Throwable $e) {
            throw new QueueException('ext-amqp: failed to count queue: ' . $e->getMessage(), 0, $e);
        }
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel === null) {
            $this->channel = new AMQPChannel($this->connection);
            $this->channel->qos(0, $this->prefetch);
        }
        return $this->channel;
    }

    private function exchange(string $name): AMQPExchange
    {
        if (isset($this->exchanges[$name])) {
            return $this->exchanges[$name];
        }

        $exchange = new AMQPExchange($this->channel());
        $exchange->setName($name);
        $exchange->setFlags(AMQP_DURABLE);

        try {
            $exchange->setType('x-delayed-message');
            $exchange->setArgument('x-delayed-type', 'direct');
            $exchange->declareExchange();
        } catch (Throwable) {
            $exchange->setType(AMQP_EX_TYPE_DIRECT);
            $exchange->declareExchange();
        }

        return $this->exchanges[$name] = $exchange;
    }

    private function brokerQueue(string $name, ?string $exchange = null, int $maxPriority = 9): AMQPBrokerQueue
    {
        $cacheKey = $name . '|' . ($exchange ?? '') . '|' . $maxPriority;
        if (isset($this->queues[$cacheKey])) {
            return $this->queues[$cacheKey];
        }

        $q = new AMQPBrokerQueue($this->channel());
        $q->setName($name);
        $q->setFlags(AMQP_DURABLE);

        if ($maxPriority > 0) {
            $q->setArgument('x-max-priority', $maxPriority);
        }

        $q->declareQueue();

        if ($exchange !== null) {
            $q->bind($exchange, $name);
        }

        return $this->queues[$cacheKey] = $q;
    }
}