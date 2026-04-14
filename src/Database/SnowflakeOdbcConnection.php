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
        $isResource = is_resource($pdo);
        $resourceType = $isResource ? get_resource_type($pdo) : 'not a resource';

        \Log::info('[ODBC LIFECYCLE] Connection constructor called', [
            'is_resource' => $isResource,
            'resource_type' => $resourceType,
            'database' => $database
        ]);

        // Store the ODBC connection resource in both places
        $this->odbcConnection = $pdo;
        $this->pdo = $pdo; // Also store in parent's property
        $this->readPdo = $pdo; // And read PDO

        // Store connection details WITHOUT calling parent (it expects PDO)
        $this->database = $database;
        $this->tablePrefix = $tablePrefix;
        $this->config = $config;

        // Set up query grammar and processor
        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();

        // Set database context — SPCS ODBC sessions don't have a default database
        if (!empty($database) && is_resource($this->odbcConnection)) {
            $quoted = '"' . str_replace('"', '""', $database) . '"';
            @odbc_exec($this->odbcConnection, "USE DATABASE {$quoted}");
            \Log::info('[ODBC LIFECYCLE] Set database context', ['database' => $database]);
        }

        \Log::info('[ODBC LIFECYCLE] Connection constructor completed', [
            'odbcConnection_valid' => is_resource($this->odbcConnection),
            'pdo_valid' => is_resource($this->pdo),
        ]);
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

            // Fetch results
            $rows = [];
            $numCols = odbc_num_fields($result);

            while (odbc_fetch_row($result)) {
                $row = [];
                for ($i = 1; $i <= $numCols; $i++) {
                    $fieldName = odbc_field_name($result, $i);
                    $fieldValue = odbc_result($result, $i);
                    $row[$fieldName] = $fieldValue;
                }
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
        \Log::info('[ODBC LIFECYCLE] odbcExecute called', [
            'odbcConnection_valid' => is_resource($this->odbcConnection),
            'odbcConnection_type' => is_resource($this->odbcConnection) ? get_resource_type($this->odbcConnection) : 'not a resource',
            'pdo_valid' => is_resource($this->pdo),
            'pdo_type' => is_resource($this->pdo) ? get_resource_type($this->pdo) : 'not a resource',
            'query_preview' => substr($query, 0, 100)
        ]);

        // Bind parameters to the query
        if (!empty($bindings)) {
            $query = $this->bindParameters($query, $bindings);
        }

        if (!is_resource($this->odbcConnection)) {
            \Log::error('[ODBC LIFECYCLE] ODBC connection resource is invalid!', [
                'odbcConnection' => $this->odbcConnection,
                'pdo' => $this->pdo,
                'readPdo' => $this->readPdo ?? 'not set'
            ]);
            throw new \Exception('ODBC connection resource is invalid');
        }

        \Log::debug('ODBC executing query', ['query' => substr($query, 0, 500), 'memory_before' => memory_get_usage(true)]);

        // Use odbc_exec directly to avoid Snowflake ODBC driver's massive pre-allocation bug
        // odbc_prepare can cause memory exhaustion (trying to allocate 64MB+) when dealing with
        // large result sets like INFORMATION_SCHEMA queries for tables/views/procedures
        $result = @odbc_exec($this->odbcConnection, $query);

        \Log::debug('ODBC query executed', ['success' => ($result !== false), 'memory_after' => memory_get_usage(true)]);

        return $result;
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
        \Log::info('[ODBC LIFECYCLE] getPdo() called', [
            'odbcConnection_valid' => is_resource($this->odbcConnection),
            'odbcConnection_type' => is_resource($this->odbcConnection) ? get_resource_type($this->odbcConnection) : 'not a resource',
            'backtrace' => debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3)
        ]);
        return $this->odbcConnection;
    }

    /**
     * Set the PDO connection (compatibility method for reconnector)
     *
     * @param  resource  $pdo
     * @return void
     */
    public function setPdo($pdo)
    {
        $isResource = is_resource($pdo);
        $resourceType = $isResource ? get_resource_type($pdo) : 'not a resource';

        \Log::info('[ODBC LIFECYCLE] setPdo() called', [
            'is_resource' => $isResource,
            'resource_type' => $resourceType,
            'backtrace' => debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 5)
        ]);

        $this->odbcConnection = $pdo;
        $this->pdo = $pdo;
        $this->readPdo = $pdo;

        \Log::info('[ODBC LIFECYCLE] setPdo() completed', [
            'odbcConnection_valid' => is_resource($this->odbcConnection)
        ]);
    }

    /**
     * Disconnect from the underlying ODBC connection.
     *
     * @return void
     */
    public function disconnect()
    {
        \Log::info('[ODBC LIFECYCLE] disconnect() called', [
            'odbcConnection_valid_before' => is_resource($this->odbcConnection),
            'backtrace' => debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 5)
        ]);

        if (is_resource($this->odbcConnection)) {
            odbc_close($this->odbcConnection);
        }

        $this->odbcConnection = null;

        \Log::info('[ODBC LIFECYCLE] disconnect() completed');
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
        \Log::info('[ODBC LIFECYCLE] reconnect() called', [
            'has_reconnector' => is_callable($this->reconnector),
            'odbcConnection_before' => is_resource($this->odbcConnection),
            'backtrace' => debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 5)
        ]);

        if (is_callable($this->reconnector)) {
            $result = call_user_func($this->reconnector, $this);
            \Log::info('[ODBC LIFECYCLE] reconnect() completed via reconnector', [
                'odbcConnection_after' => is_resource($this->odbcConnection)
            ]);
            return $result;
        }

        \Log::error('[ODBC LIFECYCLE] reconnect() failed - no reconnector available');
        throw new \LogicException('Lost ODBC connection and no reconnector available.');
    }
}
