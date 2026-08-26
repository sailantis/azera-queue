<?php

declare(strict_types=1);

namespace Azera\Queue\Backend\Amqp;

use Azera\Queue\QueueException;
use Throwable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * AMQP transport backed by the pure-PHP `php-amqplib` library.
 *
 * Requires `composer require php-amqplib/php-amqplib`. Classes are
 * namespaced under `PhpAmqpLib\`.
 */
final class PhpAmqpLibTransport implements AmqpTransport
{
    private ?AMQPChannel $channel = null;

    /** @var array<string, bool> */
    private array $exchangesDeclared = [];

    /** @var array<string, bool> */
    private array $queuesDeclared = [];

    public function __construct(
        private AbstractConnection $connection,
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
        $headers = [];
        if ($delayMs !== null && $delayMs > 0) {
            $headers['x-delay'] = $delayMs;
        }

        $message = new AMQPMessage($body, [
            'delivery_mode'       => 2,
            'priority'            => $priority,
            'message_id'          => $messageId,
            'application_headers' => new AMQPTable($headers),
        ]);

        try {
            $this->channel()->basic_publish($message, $exchange, $routingKey);
        } catch (Throwable $e) {
            throw new QueueException('php-amqplib: failed to publish: ' . $e->getMessage(), 0, $e);
        }
    }

    public function get(string $queue): ?array
    {
        try {
            $this->declareQueueInternal($queue);
            $message = $this->channel()->basic_get($queue, false);
        } catch (Throwable $e) {
            throw new QueueException('php-amqplib: failed to get message: ' . $e->getMessage(), 0, $e);
        }

        if ($message === null) {
            return null;
        }

        return [
            'body'        => $message->getBody(),
            'tag'         => (string) $message->getDeliveryTag(),
            'messageId'   => $message->get('message_id') ?? (string) $message->getDeliveryTag(),
            'redelivered' => $message->isRedelivered(),
        ];
    }

    public function ack(string $queue, string $tag): void
    {
        try {
            $this->channel()->basic_ack((int) $tag);
        } catch (Throwable $e) {
            throw new QueueException('php-amqplib: failed to ack: ' . $e->getMessage(), 0, $e);
        }
    }

    public function nack(string $queue, string $tag, bool $requeue): void
    {
        try {
            $this->channel()->basic_nack((int) $tag, false, $requeue);
        } catch (Throwable $e) {
            throw new QueueException('php-amqplib: failed to nack: ' . $e->getMessage(), 0, $e);
        }
    }

    public function declareExchange(string $name): void
    {
        $this->declareExchangeInternal($name);
    }

    public function declareQueue(string $queue, string $exchange, int $maxPriority): void
    {
        $this->declareExchangeInternal($exchange);
        $this->declareQueueInternal($queue, $exchange, $maxPriority);
    }

    public function count(string $queue): int
    {
        try {
            $this->declareQueueInternal($queue);
            [$ready, $unacked] = $this->channel()->queue_declare($queue, true);
            return $ready + $unacked;
        } catch (Throwable $e) {
            throw new QueueException('php-amqplib: failed to count queue: ' . $e->getMessage(), 0, $e);
        }
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel === null) {
            $this->channel = $this->connection->channel();
            $this->channel->basic_qos(0, $this->prefetch, false);
        }
        return $this->channel;
    }

    private function declareExchangeInternal(string $name): void
    {
        if (isset($this->exchangesDeclared[$name])) {
            return;
        }

        $ch = $this->channel();

        try {
            $ch->exchange_declare($name, 'x-delayed-message', false, true, false, false, false, new AMQPTable(['x-delayed-type' => 'direct']));
        } catch (Throwable) {
            $ch->exchange_declare($name, 'direct', false, true, false);
        }

        $this->exchangesDeclared[$name] = true;
    }

    private function declareQueueInternal(string $name, ?string $exchange = null, int $maxPriority = 9): void
    {
        $cacheKey = $name . '|' . ($exchange ?? '') . '|' . $maxPriority;
        if (isset($this->queuesDeclared[$cacheKey])) {
            return;
        }

        $ch = $this->channel();

        $args = $maxPriority > 0 ? new AMQPTable(['x-max-priority' => $maxPriority]) : null;
        $ch->queue_declare($name, true, false, false, false, false, $args);

        if ($exchange !== null) {
            $ch->queue_bind($name, $exchange, $name);
        }

        $this->queuesDeclared[$cacheKey] = true;
    }
}