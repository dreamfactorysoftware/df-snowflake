<?php

namespace DreamFactory\Core\Database\Testing;

use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Testing\TestCase;
use DreamFactory\Core\Models\Service;
use ServiceManager;
use Config;

class ExampleTest extends TestCase
{
    const RESOURCE = 'service';

    protected $serviceId = 'example';

    protected $types = [];

    public function tearDown()
    {
        Service::whereName('test-skeleton')->delete();

        parent::tearDown();
    }

    public function testExampleService()
    {
        $rs = $this->makeRequest(Verbs::GET, static::RESOURCE, ['fields' => '*'], ['resource' => []]);
        $this->assertEquals(201, $rs->getStatusCode());
        $this->assertEquals('You sent a GET request to DF example service!', 'You sent a GET request to DF example service!'); // todo: add response check
    }
}