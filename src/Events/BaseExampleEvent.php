<?php
namespace DreamFactory\Core\Events;

use DreamFactory\Core\Skeleton\Models\ExampleConfig;
use Illuminate\Queue\SerializesModels;

abstract class BaseExampleEvent
{
    use SerializesModels;

    public $example;

    /**
     * Create a new event instance.
     *
     * @param ExampleConfig $example
     */
    public function __construct(ExampleConfig $example)
    {
        $this->example = $example;
    }
}

