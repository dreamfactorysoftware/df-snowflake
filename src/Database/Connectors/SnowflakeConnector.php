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
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ];

    public function connect(array $config)
    {
        $options = array_merge($this->getOptions($config), $this->options);
        $this->checkUrlParams($config);
        $this->checkHeaders($config);
        $dsn = $this->getDsn($config);
        $connection = $this->createConnection($dsn, $config, $options);

        return $connection;
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param array $config
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

        if (empty($warehouse)) {
            throw new \InvalidArgumentException("Warehouse not given, required.");
        } else {
            $dsn .= "warehouse={$warehouse};";
        }

        if (!empty($role)) {
            $dsn .= "role={$role};";
        }

        return $dsn;
    }

    protected function checkHeaders(&$config)
    {
        $this->substituteConfig('account', 'header', $config);
        $this->substituteConfig('database', 'header', $config);
        $this->substituteConfig('schema', 'header', $config);
        $this->substituteConfig('warehouse', 'header', $config);
        $this->substituteConfig('username', 'header', $config);
        $this->substituteConfig('password', 'header', $config);
    }

    protected function checkUrlParams(&$config)
    {
        $this->substituteConfig('account', 'url', $config);
        $this->substituteConfig('database', 'url', $config);
        $this->substituteConfig('schema', 'url', $config);
        $this->substituteConfig('warehouse', 'url', $config);
        $this->substituteConfig('username', 'url', $config);
        $this->substituteConfig('password', 'url', $config);
    }

    protected function substituteConfig($name, $parameter, &$config)
    {
        switch ($parameter) {
            case 'header':
            {
                if (request()->hasHeader($name) && !empty(request()->header($name))) {
                    $config[$name] = request()->header($name);
                }
                break;
            }
            case 'url':
            {
                if (request()->has($name) && !empty(request()->query($name))) {
                    $config[$name] = request()->query($name);
                }
                break;
            }
        }
    }
}
