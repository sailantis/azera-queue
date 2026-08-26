<?php

declare(strict_types=1);

namespace Azera\Queue\Event;

/**
 * Base for queue lifecycle events.
 *
 * All queue events are value objects dispatched through the PSR-14
 * {@see \Psr\EventDispatcher\EventDispatcherInterface}, mirroring the
 * framework's DB event pattern. Listeners can log, meter, or react to
 * the job lifecycle without coupling to a specific backend.
 */
abstract class QueueEvent
{
}