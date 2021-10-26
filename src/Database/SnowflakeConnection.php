<?php

namespace DreamFactory\Core\Snowflake\Database;

use DreamFactory\Core\Snowflake\Database\Query\Grammars\SnowflakeGrammar;
use DreamFactory\Core\Snowflake\Database\Query\Processors\SnowflakeProcessor;
use DreamFactory\Core\Snowflake\Database\Schema\Grammars\SnowflakeGrammar as SchemaGrammar;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use PDO;

class SnowflakeConnection extends Connection
{
    /**
     * The Snowflake connection handler.
     *
     * @var PDO
     */
    protected $connection;

    /**
     * The name of the default schema.
     *
     * @var string
     */
    protected $defaultSchema;

    /**
     * Create a new database connection instance.
     *
     * @param  array $config
     */
    public function __construct(PDO $pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        parent::__construct($pdo, $database, $tablePrefix, $config);

        if (isset($config['schema'])) {
            $this->currentSchema = $this->defaultSchema = strtoupper($config['schema']);
        }
    }

    /**
     * Get the name of the default schema.
     *
     * @return string
     */
    public function getDefaultSchema()
    {
        return $this->defaultSchema;
    }

    /**
     * Reset to default the current schema.
     *
     * @return string
     */
    public function resetCurrentSchema()
    {
        $this->setCurrentSchema($this->getDefaultSchema());
    }

    /**
     * Set the name of the current schema.
     *
     * @param $schema
     *
     * @return string
     */
    public function setCurrentSchema($schema)
    {
        $this->statement('SET SCHEMA ?', [strtoupper($schema)]);
    }


    /**
     * Get the default query grammar instance
     *
     * @return Query\Grammars\SnowflakeGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new SnowflakeGrammar;
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processors\SnowflakeProcessor
     */
    protected function getDefaultPostProcessor()
    {
        return new SnowflakeProcessor;
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar());
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param  string $table
     * @return Builder
     */
    public function table($table, $as = null)
    {
        $processor = $this->getPostProcessor();

        $query = new Builder($this, $this->getQueryGrammar(), $processor);

        return $query->from($table, $as);
    }
}