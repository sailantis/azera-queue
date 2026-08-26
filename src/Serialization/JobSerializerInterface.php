<?php

declare(strict_types=1);

namespace Azera\Queue\Serialization;

use Azera\Queue\QueueException;

/**
 * Serializes and deserializes queue jobs.
 *
 * Async backends receive job instances by value: they must be converted
 * to a string for transport and reconstructed on the worker side. This
 * interface decouples the serialization strategy from the backend.
 *
 * Implementations MUST throw {@see QueueException} on failure (invalid
 * input, disallowed class, deserialization mismatch).
 */
interface JobSerializerInterface
{
    /**
     * @param object $job The job instance to serialize.
     * @return string A transport-safe string representation.
     * @throws QueueException
     */
    public function serialize(object $job): string;

    /**
     * @param string $payload The serialized job.
     * @return object The reconstructed job instance.
     * @throws QueueException
     */
    public function deserialize(string $payload): object;
}