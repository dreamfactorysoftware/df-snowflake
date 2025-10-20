<?php

namespace DreamFactory\Core\Snowflake\Database;

use Illuminate\Database\Connection;
use DreamFactory\Core\Snowflake\Database\Query\Grammars\SnowflakeGrammar as QueryGrammar;
use DreamFactory\Core\Snowflake\Database\Schema\Grammars\SnowflakeGrammar as SchemaGrammar;
use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use DreamFactory\Core\Snowflake\Database\Query\Processors\SnowflakeProcessor;

/**
 * Snowflake ODBC Connection
 *
 * Provides an ODBC-based connection to Snowflake that's compatible with Laravel's Connection interface
 */
class SnowflakeOdbcConnection extends Connection
{
    /**
     * The ODBC connection resource
     *
     * @var resource
     */
    protected $odbcConnection;

    /**
     * Create a new database connection instance.
     *
     * @param  resource  $pdo
     * @param  string  $database
     * @param  string  $tablePrefix
     * @param  array  $config
     * @return void
     */
    public function __construct($pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        $this->odbcConnection = $pdo;

        // Store connection details
        $this->database = $database;
        $this->tablePrefix = $tablePrefix;
        $this->config = $config;

        // Set up query grammar and processor
        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }

    /**
     * Get the default query grammar instance.
     *
     * @return \Illuminate\Database\Query\Grammars\Grammar
     */
    protected function getDefaultQueryGrammar()
    {
        return $this->withTablePrefix(new QueryGrammar);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Illuminate\Database\Schema\Grammars\Grammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar);
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Illuminate\Database\Query\Processors\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new SnowflakeProcessor;
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return \Illuminate\Database\Schema\Builder
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new SnowflakeSchema($this);
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            // Execute query using ODBC
            $result = $this->odbcExecute($query, $bindings);

            if ($result === false) {
                $error = odbc_errormsg($this->odbcConnection);
                throw new \Exception("ODBC query failed: {$error}");
            }

            // Fetch all results
            $rows = [];
            while ($row = odbc_fetch_array($result)) {
                $rows[] = (object) $row;
            }

            odbc_free_result($result);

            return $rows;
        });
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $result = $this->odbcExecute($query, $bindings);

            if ($result === false) {
                $error = odbc_errormsg($this->odbcConnection);
                throw new \Exception("ODBC statement failed: {$error}");
            }

            $affected = odbc_num_rows($result);
            odbc_free_result($result);

            return $affected === -1 ? 0 : $affected;
        });
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $result = $this->odbcExecute($query, $bindings);

            if ($result === false) {
                $error = odbc_errormsg($this->odbcConnection);
                throw new \Exception("ODBC statement failed: {$error}");
            }

            if (is_resource($result)) {
                odbc_free_result($result);
            }

            return true;
        });
    }

    /**
     * Execute a query using ODBC with parameter binding
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return resource|bool
     */
    protected function odbcExecute($query, $bindings = [])
    {
        // Bind parameters to the query
        if (!empty($bindings)) {
            $query = $this->bindParameters($query, $bindings);
        }

        return odbc_exec($this->odbcConnection, $query);
    }

    /**
     * Bind parameters to query string
     *
     * ODBC doesn't support prepared statements in the same way as PDO,
     * so we need to manually bind parameters
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return string
     */
    protected function bindParameters($query, $bindings)
    {
        foreach ($bindings as $binding) {
            // Escape and quote the binding
            $value = $this->escape($binding);

            // Replace the first ? with the escaped value
            $pos = strpos($query, '?');
            if ($pos !== false) {
                $query = substr_replace($query, $value, $pos, 1);
            }
        }

        return $query;
    }

    /**
     * Escape a value for use in a query
     *
     * @param  mixed  $value
     * @param  bool  $binary
     * @return string
     */
    public function escape($value, $binary = false)
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        // Escape single quotes by doubling them (SQL standard)
        $escaped = str_replace("'", "''", $value);
        return "'{$escaped}'";
    }

    /**
     * Get the PDO connection (compatibility method)
     *
     * Note: This returns the ODBC connection resource, not a PDO instance
     *
     * @return resource
     */
    public function getPdo()
    {
        return $this->odbcConnection;
    }

    /**
     * Disconnect from the underlying ODBC connection.
     *
     * @return void
     */
    public function disconnect()
    {
        if (is_resource($this->odbcConnection)) {
            odbc_close($this->odbcConnection);
        }

        $this->odbcConnection = null;
    }

    /**
     * Reconnect to the database.
     *
     * @return void
     *
     * @throws \LogicException
     */
    public function reconnect()
    {
        if (is_callable($this->reconnector)) {
            return call_user_func($this->reconnector, $this);
        }

        throw new \LogicException('Lost ODBC connection and no reconnector available.');
    }
}
