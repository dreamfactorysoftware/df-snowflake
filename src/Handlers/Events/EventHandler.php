<?php
namespace DreamFactory\Core\Compliance\Handlers\Events;

use DreamFactory\Core\Events\ExampleCreatingEvent;
use DreamFactory\Core\Events\UserCreatingEvent;
use DreamFactory\Core\Events\BaseExampleEvent;
use DreamFactory\Core\Events\BaseUserEvent;
use DreamFactory\Core\Skeleton\Components\ExampleComponent;
use Illuminate\Contracts\Events\Dispatcher;

class EventHandler
{
    /**
     * Register the listeners for the subscriber.
     *
     * @param  Dispatcher $events
     */
    public function subscribe($events)
    {
        // subscribe to Core event
        $events->listen(
            [
                UserCreatingEvent::class,
            ],
            static::class . '@handleUserCreatingEvent'
        );

        // subscribe to our own event
        $events->listen(
            [
                ExampleCreatingEvent::class,
            ],
            static::class . '@handleExampleCreatingEvent'
        );
    }

    /**
     * Handle User creating events.
     *
     * @param BaseUserEvent $event
     *
     * @return void
     */
    public function handleUserCreatingEvent($event)
    {
        $isExample = ExampleComponent::getExample() === 'example';

        if ($isExample) {
            $user = $event->user;
            // do something internal
        }
    }

    /**
     * Handle Example creating events.
     * Your own event
     *
     * @param BaseExampleEvent $event
     *
     * @return void
     */
    public function handleExampleCreatingEvent($event)
    {
        $isExample = ExampleComponent::getExample() === 'example';

        if ($isExample) {
            $example = $event->example;
            // do something internal
        }
    }
}