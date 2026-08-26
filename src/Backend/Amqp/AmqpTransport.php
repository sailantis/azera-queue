<?php

declare(strict_types=1);

namespace Azera\Queue\Backend\Amqp;

use Azera\Queue\QueueException;

/**
 * Minimal AMQP transport abstraction.
 *
 * Hides the differences between the two PHP RabbitMQ implementations so
 * {@see \Azera\Queue\Backend\AmqpQueue} can run on either:
 *
 *  - `ext-amqp` (PECL extension; global classes, enabled in php.ini), or
 *  - `php-amqplib/php-amqplib` (pure-PHP library; namespaced classes).
 *
 * Implementations must throw {@see QueueException} on any failure.
 */
interface AmqpTransport
{
    /**
     * Publish a message body to the exchange under a routing key.
     *
     * @param string   $exchange   Exchange name.
     * @param string   $routingKey Routing key (typically the queue name).
     * @param string   $body       Raw serialized payload.
     * @param int      $priority   AMQP priority (0-9).
     * @param int|null $delayMs    Optional delay in milliseconds.
     * @param string   $messageId  Message id (used for dedup/tracking).
     */
    public function publish(
        string $exchange,
        string $routingKey,
        string $body,
        int $priority,
        ?int $delayMs,
        string $messageId,
    ): void;

    /**
     * Fetch the next available message from $queue without blocking.
     *
     * @return array{body: string, tag: string, messageId: string, redelivered: bool}|null
     */
    public function get(string $queue): ?array;

    /**
     * Acknowledge a message by delivery tag.
     */
    public function ack(string $queue, string $tag): void;

    /**
     * Reject a message, optionally requeueing it.
     */
    public function nack(string $queue, string $tag, bool $requeue): void;

    /**
     * Declare the shared exchange, preferring a delayed-message exchange
     * when the plugin is available and falling back to direct otherwise.
     */
    public function declareExchange(string $name): void;

    /**
     * Declare a durable queue and bind it to the exchange under its name.
     */
    public function declareQueue(string $queue, string $exchange, int $maxPriority): void;

    /**
     * Count messages on $queue.
     */
    public function count(string $queue): int;
}