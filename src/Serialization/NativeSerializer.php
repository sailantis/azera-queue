<?php

declare(strict_types=1);

namespace Azera\Queue\Serialization;

use Azera\Queue\QueueException;

/**
 * Serializes jobs using PHP's native {@see serialize()}.
 *
 * Optionally restricts the set of classes allowed during deserialization
 * via `unserialize()`'s `allowed_classes` option. The allowlist is a
 * security measure: it prevents arbitrary object instantiation from
 * untrusted payloads. When no allowlist is supplied, all classes are
 * allowed (the original behavior), which is appropriate when the queue
 * payload is trusted (e.g. the queue is private and the worker is the
 * only producer).
 *
 * Closure-based jobs require a closure serializer (e.g. `opis/closure`
 * or `laravel/serializable-closure`); this class does not handle
 * closures and throws when asked to serialize one.
 */
class NativeSerializer implements JobSerializerInterface
{
    /** @var array<class-string>|null */
    private ?array $allowedClasses;

    /**
     * @param array<class-string>|null $allowedClasses When null, all
     *   classes are allowed during deserialization. When an array, only
     *   listed classes (and their dependencies) are permitted.
     */
    public function __construct(?array $allowedClasses = null)
    {
        $this->allowedClasses = $allowedClasses;
    }

    public function serialize(object $job): string
    {
        $reflection = new \ReflectionObject($job);
        foreach ($reflection->getProperties() as $property) {
            if ($property->isInitialized($job) && $property->getValue($job) instanceof \Closure) {
                throw new QueueException(sprintf(
                    'NativeSerializer cannot serialize closure jobs (in %s::$%s). Use a closure serializer or a concrete job class.',
                    $job::class,
                    $property->getName(),
                ));
            }
        }

        $serialized = \serialize($job);

        if ($serialized === false) {
            throw new QueueException('Failed to serialize job.');
        }

        return $serialized;
    }

    public function deserialize(string $payload): object
    {
        if ($payload === '') {
            throw new QueueException('Empty job payload.');
        }

        try {
            $job = \unserialize($payload, [
                'allowed_classes' => $this->allowedClasses ?? true,
            ]);
        } catch (\Throwable $e) {
            throw new QueueException('Failed to deserialize job: ' . $e->getMessage(), 0, $e);
        }

        if (!is_object($job)) {
            throw new QueueException('Deserialized payload is not an object.');
        }

        return $job;
    }
}