<?php

namespace DreamFactory\Core\Snowflake\Tests;

use DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeConnector;
use PHPUnit\Framework\TestCase;

class SnowflakeConnectorTest extends TestCase
{
    protected $connector;

    protected function setUp(): void
    {
        $this->connector = new SnowflakeConnector();
    }

    /**
     * Test private link hostname detection
     */
    public function testPrivateLinkHostnameDetection()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('isPrivateLinkHostname');
        $method->setAccessible(true);

        // Test standard private link patterns
        $this->assertTrue($method->invoke($this->connector, 'myaccount.privatelink.snowflakecomputing.com'));
        $this->assertTrue($method->invoke($this->connector, 'test.internal.snowflake.example.com'));
        $this->assertTrue($method->invoke($this->connector, 'vpce-12345abc.snowflake.us-east-1.amazonaws.com'));

        // Test regular hostnames (should return false)
        $this->assertFalse($method->invoke($this->connector, 'myaccount.snowflakecomputing.com'));
        $this->assertFalse($method->invoke($this->connector, 'regular-hostname.com'));
        $this->assertFalse($method->invoke($this->connector, 'localhost'));
    }

    /**
     * Test DSN generation for private link connections
     */
    public function testPrivateLinkDsnGeneration()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('getDsn');
        $method->setAccessible(true);

        // Test private link without account parameter (preferred)
        $privateLinkConfig = [
            'hostname' => 'myaccount.privatelink.snowflakecomputing.com',
            'database' => 'testdb',
            'schema' => 'testschema',
            'warehouse' => 'testwh',
            'role' => 'testrole',
            'private_link_enabled' => true,
            'region' => 'us-east-1'
        ];

        $dsn = $method->invoke($this->connector, $privateLinkConfig);

        // Verify DSN contains expected components but NOT account parameter for private links
        $this->assertStringContainsString('snowflake:', $dsn);
        $this->assertStringContainsString('host=myaccount.privatelink.snowflakecomputing.com', $dsn);
        $this->assertStringNotContainsString('account=', $dsn); // Should NOT contain account for private links
        $this->assertStringContainsString('database=testdb', $dsn);
        $this->assertStringContainsString('schema=testschema', $dsn);
        $this->assertStringContainsString('warehouse=testwh', $dsn);
        $this->assertStringContainsString('role=testrole', $dsn);
        $this->assertStringContainsString('region=us-east-1', $dsn);
        $this->assertStringContainsString('application=DreamFactory_DreamFactory', $dsn);
    }

    /**
     * Test private link with matching account parameter
     */
    public function testPrivateLinkWithMatchingAccount()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('getDsn');
        $method->setAccessible(true);

        $privateLinkConfig = [
            'hostname' => 'myaccount.privatelink.snowflakecomputing.com',
            'account' => 'myaccount', // This matches the hostname
            'database' => 'testdb',
            'schema' => 'testschema'
        ];

        $dsn = $method->invoke($this->connector, $privateLinkConfig);

        // Even with matching account, we shouldn't include it in DSN for private links
        $this->assertStringContainsString('host=myaccount.privatelink.snowflakecomputing.com', $dsn);
        $this->assertStringNotContainsString('account=', $dsn);
    }

    /**
     * Test account extraction from private link hostnames
     */
    public function testAccountExtractionFromPrivateLink()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('extractAccountFromPrivateLinkHostname');
        $method->setAccessible(true);

        // Test standard private link format
        $this->assertEquals('myaccount', $method->invoke($this->connector, 'myaccount.privatelink.snowflakecomputing.com'));
        $this->assertEquals('testorg-testacct', $method->invoke($this->connector, 'testorg-testacct.privatelink.snowflakecomputing.com'));
        
        // Test other private link patterns
        $this->assertEquals('myaccount', $method->invoke($this->connector, 'myaccount.internal.snowflake.example.com'));
        
        // Test non-private link hostnames
        $this->assertEquals('myaccount', $method->invoke($this->connector, 'myaccount.snowflakecomputing.com'));
        $this->assertNull($method->invoke($this->connector, 'localhost'));
    }

    /**
     * Test DSN generation for standard connections
     */
    public function testStandardDsnGeneration()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('getDsn');
        $method->setAccessible(true);

        $standardConfig = [
            'hostname' => 'myaccount.snowflakecomputing.com',
            'account' => 'myaccount',
            'database' => 'testdb',
            'schema' => 'testschema',
            'warehouse' => 'testwh'
        ];

        $dsn = $method->invoke($this->connector, $standardConfig);

        // Should not contain private link specific parameters
        $this->assertStringNotContainsString('region=', $dsn);
        $this->assertStringContainsString('host=myaccount.snowflakecomputing.com', $dsn);
    }

    /**
     * Test edge cases and validation
     */
    public function testEdgeCases()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('getDsn');
        $method->setAccessible(true);

        // Test missing schema throws exception
        $this->expectException(\InvalidArgumentException::class);
        $method->invoke($this->connector, [
            'hostname' => 'test.privatelink.snowflakecomputing.com',
            'account' => 'test'
        ]);
    }

    /**
     * Test DSN value escaping
     */
    public function testDsnValueEscaping()
    {
        $reflection = new \ReflectionClass($this->connector);
        $method = $reflection->getMethod('escapeDsnValue');
        $method->setAccessible(true);

        $this->assertEquals('test\\;value', $method->invoke($this->connector, 'test;value'));
        $this->assertEquals('test\\=value', $method->invoke($this->connector, 'test=value'));
        $this->assertEquals('normal', $method->invoke($this->connector, 'normal'));
    }
} 