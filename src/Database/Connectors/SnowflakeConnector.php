<?php

namespace DreamFactory\Core\Snowflake\Database\Connectors;

use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

class SnowflakeConnector extends Connector implements ConnectorInterface
{
    /**
     * The PDO connection options.
     *
     * @var array
     */
    protected $options = [
        PDO::ATTR_ERRMODE           => PDO::ERRMODE_EXCEPTION
    ];

    public function connect(array $config)
    {
        $options = array_merge($this->getOptions($config), $this->options);
        $dsn = $this->getDsn($config);
        $connection = $this->createConnection($dsn, $config, $options);

        return $connection;
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        $dsn = "snowflake:";

        if (empty($account)) {
            throw new \InvalidArgumentException("Account not given, required.");
        } else {
            $dsn .= "account={$account};";
        }

        if (empty($database)) {
            throw new \InvalidArgumentException("Database not given, required.");
        } else {
            $dsn .= "database={$database};";
        }

        if (!empty($schema)) {
            $dsn .= "schema={$schema};";
        } else {
            $schema = strtoupper(SnowflakeSchema::DEFAULT_SCHEMA);
            $dsn .= "schema={$schema};";
        }

        if (!empty($warehouse)) {
            $dsn .= "warehouse={$warehouse};";
        } else {
            throw new \InvalidArgumentException("Warehouse not given, required.");
        }

        if (!empty($role)) {
            $dsn .= "role={$role};";
        }

        return $dsn;
    }
}
