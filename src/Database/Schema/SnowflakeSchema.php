<?php

namespace DreamFactory\Core\Snowflake\Database\Schema;

use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeFunctionSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use Arr;

class SnowflakeSchema extends SqlSchema
{
    /**
     * Flag to indicate if we're currently processing function parameters that may need JSON encoding
     * @var bool
     */
    protected $processingFunctionParameters = true;

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        // Check if we're using ODBC connection - SHOW commands cause memory issues with ODBC
        $isOdbc = is_resource($this->connection->getPdo());

        if ($isOdbc) {
            // Use INFORMATION_SCHEMA for ODBC connections
            $sql = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = \'BASE TABLE\'';
            if (!empty($schema)) {
                // For ODBC, manually escape the value since quoteValue() expects PDO
                $escapedSchema = str_replace("'", "''", $schema);
                $sql .= ' AND TABLE_SCHEMA = \'' . $escapedSchema . '\'';
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = (array)$row;
                $schemaName = $schema;
                $resourceName = $row['TABLE_NAME'];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new TableSchema($settings);
            }
        } else {
            // Use SHOW TABLES for PDO connections
            $sql = 'SHOW TABLES ';

            if (!empty($schema)) {
                $sql .= ' IN ' . $this->quoteTableName($schema);
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = array_values((array)$row);
                $schemaName = $schema;
                $resourceName = $row[1];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new TableSchema($settings);
            }
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    public function getProcedureNames($schema = '')
    {
        // Check if we're using ODBC connection - SHOW commands cause memory issues with ODBC
        $isOdbc = is_resource($this->connection->getPdo());

        if ($isOdbc) {
            // Use INFORMATION_SCHEMA for ODBC connections
            $sql = 'SELECT PROCEDURE_NAME FROM INFORMATION_SCHEMA.PROCEDURES';
            if (!empty($schema)) {
                $escapedSchema = str_replace("'", "''", $schema);
                $sql .= ' WHERE PROCEDURE_SCHEMA = \'' . $escapedSchema . '\'';
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = (array)$row;
                $schemaName = $schema;
                $resourceName = $row['PROCEDURE_NAME'];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new ProcedureSchema($settings);
            }
        } else {
            // Use SHOW PROCEDURES for PDO connections
            $sql = 'SHOW PROCEDURES ';

            if (!empty($schema)) {
                $sql .= ' IN ' . $this->quoteTableName($schema);
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = array_values((array)$row);
                $schemaName = $schema;
                $resourceName = $row[1];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new ProcedureSchema($settings);
            }
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames($schema = '')
    {
        // Check if we're using ODBC connection - SHOW commands cause memory issues with ODBC
        $isOdbc = is_resource($this->connection->getPdo());

        if ($isOdbc) {
            // Use INFORMATION_SCHEMA for ODBC connections
            $sql = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS';
            if (!empty($schema)) {
                $escapedSchema = str_replace("'", "''", $schema);
                $sql .= ' WHERE TABLE_SCHEMA = \'' . $escapedSchema . '\'';
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = (array)$row;
                $schemaName = $schema;
                $resourceName = $row['TABLE_NAME'];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new TableSchema($settings);
            }
        } else {
            // Use SHOW VIEWS for PDO connections
            $sql = 'SHOW VIEWS ';

            if (!empty($schema)) {
                $sql .= ' IN ' . $this->quoteTableName($schema);
            }

            $rows = $this->connection->select($sql);

            $names = [];
            foreach ($rows as $row) {
                $row = array_values((array)$row);
                $schemaName = $schema;
                $resourceName = $row[1];
                $internalName = $schemaName . '.' . $resourceName;
                $name = $resourceName;
                $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
                $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
                $names[strtolower($name)] = new TableSchema($settings);
            }
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineParamString(array $param_schemas, array &$values)
    {
        $paramParts = [];
        foreach ($param_schemas as $key => $paramSchema) {
            $paramTypeUpper = strtoupper($paramSchema->paramType);
            if ($paramTypeUpper === 'IN' || $paramTypeUpper === 'INOUT') {
                $placeholder = ':' . $paramSchema->name;

                $dbTypeUpper = strtoupper($paramSchema->dbType);
                if ($dbTypeUpper === 'ARRAY' || $dbTypeUpper === 'OBJECT' || $dbTypeUpper === 'VARIANT') {
                    $paramParts[] = 'PARSE_JSON(' . $placeholder . ')';
                } else {
                    $paramParts[] = $placeholder;
                }
            }
        }
        return implode(', ', $paramParts);
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineNames($type, $schema = '')
    {
        $bindings = [];
        $where = $type . '_SCHEMA = :schema';
        if (!empty($schema)) {
            $bindings[':schema'] = $schema;
        }
        $sql = <<<MYSQL
SELECT {$type}_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.{$type}S WHERE {$where}
MYSQL;

        $rows = $this->connection->select($sql, $bindings);

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $resourceName = Arr::get($row, $type . '_NAME');
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $returnType = Arr::get($row, 'DATA_TYPE');
            if (!empty($returnType) && (0 !== strcasecmp('void', $returnType))) {
                $returnType = static::extractSimpleType($returnType);
            }
            $settings = compact('schemaName', 'resourceName', 'name', 'quotedName', 'internalName', 'returnType');
            $names[strtolower($name)] =
                ('PROCEDURE' === $type) ? new ProcedureSchema($settings) : new SnowflakeFunctionSchema($settings);
        }
        return $names;
    }

    public function getSchemas()
    {
        $sql = <<<SQL
SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
SQL;
        $rows = $this->selectColumn($sql);
        return $rows;
    }

    /**
     * @inheritdoc
     */
    public function callProcedure($procedure, array $in_params, array &$out_params)
    {
        if (!$this->supportsResourceType(DbResourceTypes::TYPE_PROCEDURE)) {
            throw new BadRequestException('Stored Procedures are not supported by this database connection.');
        }

        $paramSchemas = $procedure->getParameters();
        $values = $this->determineRoutineValues($paramSchemas, $in_params);

        $sql = $this->getProcedureStatement($procedure, $paramSchemas, $values);

        /** @type \PDOStatement $statement */
        if (!$statement = $this->connection->getPdo()->prepare($sql)) {
            throw new InternalServerErrorException('Failed to prepare statement: ' . $sql);
        }

        // do binding
        $this->doRoutineBinding($statement, $paramSchemas, $values);

        // support multiple result sets
        $result = [];
        try {
            $statement->execute();
            $reader = new DataReader($statement);
            $reader->setFetchMode(static::ROUTINE_FETCH_MODE);
            try {
                if (0 < $reader->getColumnCount()) {
                    $temp = $reader->readAll();
                }
            } catch (\Exception $ex) {
                // latest oracle driver seems to kick this back for all OUT params even though it works, ignore for now
                if (false === stripos($ex->getMessage(),
                        'ORA-24374: define not done before fetch or execute and fetch')
                ) {
                    throw $ex;
                }
            }
            if (!empty($temp)) {
                $result[] = $temp;
            }
        } catch (\Exception $ex) {
            if (!$this->handleRoutineException($ex)) {
                $errorInfo = $ex instanceof \PDOException ? $ex : null;
                $message = $ex->getMessage();
                throw new \Exception($message, (int)$ex->getCode(), $errorInfo);
            }
        }

        // if there is only one data set, just return it
        if (1 == count($result)) {
            $result = $result[0];
        }

        // any post op?
        $this->postProcedureCall($paramSchemas, $values);

        $values = array_change_key_case($values, CASE_LOWER);
        foreach ($paramSchemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'OUT':
                case 'INOUT':
                    if (array_key_exists($key, $values)) {
                        $value = $values[$key];
                        $out_params[$paramSchema->name] = $this->typecastToClient($value, $paramSchema);
                    }
                    break;
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function loadParameters(RoutineSchema $holder)
    {
        if (str_contains($holder::class, 'Procedure')) {
            $type = 'PROCEDURE';
        } else $type = 'FUNCTION';

        $dbNamePrefix = '';
        if ($holder instanceof SnowflakeFunctionSchema && !empty($holder->databaseName)) {
            $dbNamePrefix = $this->quoteTableName($holder->databaseName) . '.';
        }
 
        $sql = <<<MYSQL
SELECT * FROM {$dbNamePrefix}INFORMATION_SCHEMA.{$type}S WHERE {$type}_NAME = '{$holder->resourceName}' AND {$type}_SCHEMA = '{$holder->schemaName}'
MYSQL;

        $bindings = [':object' => $type, ':schema' => $holder->schemaName];
 
        $rows = $this->connection->select($sql, $bindings);
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $argumentSignature = str_replace(['(', ')'], '"', Arr::get($row, 'ARGUMENT_SIGNATURE'));
            $arguments = [];
            eval('$arguments = explode( ", ", ' . $argumentSignature . ');');
            foreach ($arguments as $key => $value) {
                $pos = intval($key + 1);
                // parse ARGUMENT_SIGNATURE
                $argument = explode(' ', $value);
                $argument['name'] = $argument[0] ?? null;
                $argument['type'] = $argument[1] ?? null;
                $name = $argument['name'];
                $simpleType = static::extractSimpleType($argument['type']);
                if (empty($name)) {
                    $holder->returnType = Arr::get($row, 'DATA_TYPE');
                } else {
                    $holder->addParameter(new ParameterSchema([
                            'name' => $name,
                            'position' => $pos,
                            // Snowflake supports only INPUT arguments
                            'param_type' => 'IN',
                            'type' => $simpleType,
                            'db_type' => $argument['type'],
                            'length' => (isset($row['CHARACTER_MAXIMUM_LENGTH']) ? intval(Arr::get($row, 'CHARACTER_MAXIMUM_LENGTH')) : null),
                            'precision' => (isset($row['NUMERIC_PRECISION']) ? intval(Arr::get($row, 'NUMERIC_PRECISION'))
                                : null),
                            'scale' => (isset($row['NUMERIC_SCALE']) ? intval(Arr::get($row, 'NUMERIC_SCALE')) : null),
                        ]
                    ));
                }
            }
            if ($type === 'FUNCTION' && empty($arguments) && empty($holder->returnType)){
                 $returnDbType = Arr::get($row, 'DATA_TYPE');
                 if (!empty($returnDbType)) {
                     $holder->returnType = static::extractSimpleType($returnDbType);
                     // Only set returnDbtype for SnowflakeFunctionSchema instances
                     if ($holder instanceof SnowflakeFunctionSchema) {
                         $holder->returnDbtype = $returnDbType;
                     }
                 }
            }
        }
    }

    /**
     * @param FunctionSchema $function
     * @param array $in_params
     *
     * @return mixed
     * @throws \Exception
     */
    public function callFunction($function, array $in_params)
    {
        if (!$this->supportsResourceType(DbResourceTypes::TYPE_FUNCTION)) {
            throw new \Exception('Stored Functions are not supported by this database connection.');
        }

        $paramSchemas = $function->getParameters();
        
        \Log::info('Function Param Schemas: ' . json_encode($paramSchemas));

        // Handle Cortex functions, since some provides multiple ways of specifing params
        if ($this->isCortexFunction($function)) {
            $paramSchemas = $this->createDynamicParameterSchemas($in_params);
            \Log::info('Created dynamic parameter schemas for Cortex function: ' . json_encode($paramSchemas));
        }

        // Set flag to enable Snowflake-specific processing in typecastToClient
        $this->processingFunctionParameters = true;
        $values = $this->determineRoutineValues($paramSchemas, $in_params);
        $this->processingFunctionParameters = false;

        $sql = $this->getFunctionStatement($function, $paramSchemas, $values);

        \Log::info('SQL Query: ' . $sql);

        /** @type \PDOStatement $statement */
        if (!$statement = $this->connection->getPdo()->prepare($sql)) {
            throw new InternalServerErrorException('Failed to prepare statement: ' . $sql);
        }

        $this->doRoutineBinding($statement, $paramSchemas, $values);

        // support multiple result sets
        $result = [];
        try {
            $statement->execute();
            $reader = new DataReader($statement);
            $reader->setFetchMode(static::ROUTINE_FETCH_MODE);
            $temp = $reader->readAll();
            if (!empty($temp)) {
                $result[] = $temp;
            }
        } catch (\Exception $ex) {
            if (!$this->handleRoutineException($ex)) {
                $errorInfo = $ex instanceof \PDOException ? $ex : null;
                $message = $ex->getMessage();
                throw new \Exception($message, (int)$ex->getCode(), $errorInfo);
            }
        }

        // if there is only one data set, just return it
        if (1 == count($result)) {
            $result = $result[0];
            // if there is only one data set, search for an output
            if (1 == count($result)) {
                $result = current($result);
                if (array_key_exists('output', $result)) {
                    $value = $result['output'];

                    return $this->typecastToClient($value, $function->returnType);
                } elseif (array_key_exists($function->name, $result)) {
                    // some vendors return the results as the function's name
                    $value = $result[$function->name];

                    return $this->typecastToClient($value, $function->returnType);
                }
            }
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        // Check if we're using ODBC connection
        $isOdbc = is_resource($this->connection->getPdo());

        if ($isOdbc) {
            // Use INFORMATION_SCHEMA for ODBC connections to avoid SHOW commands
            // Parse schema and table name from quotedName
            $parts = explode('.', str_replace(['"', '`', '[', ']'], '', $table->quotedName));
            $schemaName = count($parts) > 1 ? $parts[0] : $table->schemaName;
            $tableName = count($parts) > 1 ? $parts[1] : $parts[0];

            // Manually escape values since quoteValue() expects PDO
            $escapedSchema = str_replace("'", "''", $schemaName);
            $escapedTable = str_replace("'", "''", $tableName);

            $sql = <<<SQL
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    COMMENT,
    ORDINAL_POSITION,
    IS_IDENTITY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{$escapedSchema}'
AND TABLE_NAME = '{$escapedTable}'
ORDER BY ORDINAL_POSITION
SQL;

            \Log::info('Loading table columns for ODBC using INFORMATION_SCHEMA', [
                'table' => $table->quotedName,
                'schema' => $schemaName,
                'table_name' => $tableName
            ]);

            $result = $this->connection->select($sql);

            foreach ($result as $column) {
                $column = array_change_key_case((array)$column, CASE_LOWER);
                $c = new ColumnSchema(['name' => $column['column_name']]);
                $c->quotedName = $this->quoteColumnName($c->name);
                $c->allowNull = ($column['is_nullable'] === 'YES');
                $c->dbType = $column['data_type'];
                $c->autoIncrement = isset($column['is_identity']) && $column['is_identity'] === 'YES';

                if (isset($column['comment']) && !empty($column['comment'])) {
                    $c->comment = $column['comment'];
                }

                // Set size/precision based on data type
                if (isset($column['character_maximum_length'])) {
                    $c->size = (int)$column['character_maximum_length'];
                }
                if (isset($column['numeric_precision'])) {
                    $c->precision = (int)$column['numeric_precision'];
                }
                if (isset($column['numeric_scale'])) {
                    $c->scale = (int)$column['numeric_scale'];
                }

                $this->extractLimit($c, $c->dbType);
                $c->fixedLength = $this->extractFixedLength($c->dbType);
                $this->extractType($c, $c->dbType);
                $this->extractDefault($c, $column['column_default']);

                // Note: Primary key info not available via INFORMATION_SCHEMA.COLUMNS in Snowflake
                // Would need separate query to INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                // For now, auto-increment columns are likely primary keys
                if ($c->autoIncrement) {
                    $c->isPrimaryKey = true;
                    $table->addPrimaryKey($c->name);
                    if ((DbSimpleTypes::TYPE_INTEGER === $c->type)) {
                        $c->type = DbSimpleTypes::TYPE_ID;
                    }
                }

                $table->addColumn($c);
            }
        } else {
            // Use SHOW COLUMNS for PDO connections (original code)
            $this->connection->statement('show columns in table ' . $table->quotedName);
            $this->connection->statement('set s_c=last_query_id();');
            $this->connection->statement('desc table ' . $table->quotedName);
            $this->connection->statement('set d_t=last_query_id();');

            $sql = <<<SQL
select d.*, s."autoincrement" from table(result_scan(\$d_t)) as d
JOIN table(result_scan(\$s_c)) as s
ON d."name" = s."column_name";
SQL;

            $result = $this->connection->select($sql);
            foreach ($result as $column) {
                $column = array_change_key_case((array)$column, CASE_LOWER);
                $c = new ColumnSchema(['name' => $column['name']]);
                $c->quotedName = $this->quoteColumnName($c->name);
                $c->allowNull = $column['null?'] === 'Y';
                $c->isPrimaryKey = str_contains($column['primary key'], 'Y');
                $c->isUnique = str_contains($column['unique key'], 'Y');
                $c->autoIncrement = isset($column['autoincrement']) && $column['autoincrement'] !== '' ? true : false;
                $c->dbType = $column['type'];
                if (isset($column['comment'])) {
                    $c->comment = $column['comment'];
                }
                $this->extractLimit($c, $c->dbType);
                $c->fixedLength = $this->extractFixedLength($c->dbType);
                $this->extractType($c, $c->dbType);
                $this->extractDefault($c, $column['default']);

                if ($c->isPrimaryKey) {
                    if ($c->autoIncrement) {
                        $table->sequenceName = Arr::get($column, 'sequence', $c->name);
                        if ((DbSimpleTypes::TYPE_INTEGER === $c->type)) {
                            $c->type = DbSimpleTypes::TYPE_ID;
                        }
                    }
                    $table->addPrimaryKey($c->name);
                }
                $table->addColumn($c);
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        if (is_array($schema)) {
            $schema = implode("','", $schema);
        }

        // Check if we're using ODBC connection
        $isOdbc = is_resource($this->connection->getPdo());

        if ($isOdbc) {
            // For ODBC, just return empty constraints to avoid SHOW commands
            // Snowflake doesn't have KEY_COLUMN_USAGE in INFORMATION_SCHEMA
            // This means we won't have FK/PK info for ODBC connections, but it will work
            return [];
        } else {
            // For PDO, use original SHOW commands approach
            $this->connection->statement('SHOW PRIMARY KEYS;');
            $this->connection->statement('set pk_id=last_query_id();');
            $this->connection->statement('SHOW IMPORTED KEYS;');
            $this->connection->statement('set fk_id=last_query_id();');

            $sql = <<<SQL
SELECT tc.constraint_type, tc.constraint_schema, tc.constraint_name, tc.table_schema, tc.table_name,
kcu."column_name", kcu."referenced_table_schema", kcu."referenced_table_name", kcu."referenced_column_name",
rc.update_rule, rc.delete_rule
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN (
select fk."fk_schema_name" as "constraint_schema", fk."fk_name" as "constraint_name", fk."fk_schema_name" as "table_schema", fk."fk_table_name" as "table_name", fk."fk_column_name" as "column_name",
fk."pk_schema_name" as "referenced_table_schema", fk."pk_table_name" as "referenced_table_name", fk."pk_column_name" as "referenced_column_name", fk."update_rule" as "update_rule", fk."delete_rule" as "delete_rule" from table(result_scan(\$fk_id)) as fk
UNION ALL
select pk."schema_name" as "constraint_schema", pk."constraint_name" as "constraint_name", pk."schema_name" as "table_schema", pk."table_name" as "table_name", pk."column_name" as "column_name",
null as "referenced_table_schema", null as "referenced_table_name", null as "referenced_column_name", null as "update_rule", null as "delete_rule"
from table(result_scan(\$pk_id)) as pk
) as kcu ON tc.constraint_name = kcu."constraint_name" AND tc.table_schema = kcu."constraint_schema" AND tc.table_name = kcu."table_name"
LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON tc.constraint_schema = rc.constraint_schema AND
tc.constraint_name = rc.constraint_name
WHERE tc.constraint_schema IN ('{$schema}');
SQL;
        }

        $results = $this->connection->select($sql);
        $constraints = [];
        foreach ($results as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $ts = strtolower($row['table_schema']);
            $tn = strtolower($row['table_name']);
            $cn = strtolower($row['constraint_name']);
            $colName = Arr::get($row, 'column_name');
            $refColName = Arr::get($row, 'referenced_column_name');
            if (isset($constraints[$ts][$tn][$cn])) {
                $constraints[$ts][$tn][$cn]['column_name'] =
                    array_merge((array)$constraints[$ts][$tn][$cn]['column_name'], (array)$colName);

                if (isset($refColName)) {
                    $constraints[$ts][$tn][$cn]['referenced_column_name'] =
                        array_merge((array)$constraints[$ts][$tn][$cn]['referenced_column_name'], (array)$refColName);
                }
            } else {
                $constraints[$ts][$tn][$cn] = $row;
            }
        }

        return $constraints;
    }

    /**
     * @param \DreamFactory\Core\Database\Schema\RoutineSchema $routine
     * @param array                                            $param_schemas
     * @param array                                            $values
     *
     * @return string
     */
    protected function getFunctionStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = $this->getRoutineParamString($param_schemas, $values);
        
        $funcNameParts = [];
        if ($routine instanceof SnowflakeFunctionSchema && !empty($routine->databaseName)) {
            $funcNameParts[] = $this->quoteTableName($routine->databaseName);
        }
        if (!empty($routine->schemaName)) {
            $funcNameParts[] = $this->quoteTableName($routine->schemaName);
        }
        $funcNameParts[] = $this->quoteTableName($routine->resourceName);
        
        $fullyQualifiedQuotedFuncName = implode('.', $funcNameParts);

        return "SELECT {$fullyQualifiedQuotedFuncName}($paramStr) AS " . $this->quoteColumnName('output');
    }

    /**
     * Check if this is a CORTEX function
     *
     * @param FunctionSchema $function
     * @return bool
     */
    protected function isCortexFunction($function)
    {
        $schemaName = isset($function->schemaName) ? strtoupper($function->schemaName) : '';
        $resourceName = isset($function->resourceName) ? strtoupper($function->resourceName) : '';

        // List of known Cortex functions (extend if needed)
        $knownCortexFunctions = [
            'COMPLETE',
            'CLASSIFY_TEXT',
            'EXTRACT_ANSWER',
            'SENTIMENT',
            'SUMMARIZE',
            'TRANSLATE',
            'EMBED_TEXT',
            'EMBED_TEXT_768',
            'EMBED_TEXT_1024',
            'PARSE_DOCUMENT',
            'SPLIT_TEXT_RECURSIVE_CHARACTER',
            'ANALYST_PREVIEW'
        ];

        return (
            $schemaName === 'CORTEX' || 
            $schemaName === 'SNOWFLAKE.CORTEX' || 
            str_contains($resourceName, 'CORTEX') ||
            in_array($resourceName, $knownCortexFunctions)
        );
    }

    /**
     * Create dynamic parameter schemas based on the input parameters provided
     *
     * @param array $in_params
     * @return array
     */
    protected function createDynamicParameterSchemas(array $in_params)
    {
        \Log::debug($in_params);
        $paramSchemas = [];
        $position = 1;
        
        foreach ($in_params as $key => $param) {
            $paramName = '';
            $paramValue = null;
            
            // Handle different parameter formats
            if (is_array($param)) {
                // Format: [['name' => 'param1', 'value' => 'value1'], ...]
                $paramName = array_get($param, 'name', 'param' . $position);
                $paramValue = array_get($param, 'value');
            } else {
                // Format: ['param1' => 'value1', 'param2' => 'value2', ...]
                $paramName = is_string($key) ? $key : 'param' . $position;
                $paramValue = $param;
            }
            
            // Determine parameter type based on value
            $paramType = 'string'; // default
            $dbType = 'VARCHAR';
            
            if (is_numeric($paramValue)) {
                if (is_int($paramValue) || ctype_digit($paramValue)) {
                    $paramType = 'integer';
                    $dbType = 'NUMBER';
                } else {
                    $paramType = 'float';
                    $dbType = 'FLOAT';
                }
            } elseif (is_bool($paramValue)) {
                $paramType = 'boolean';
                $dbType = 'BOOLEAN';
            } elseif (is_array($paramValue) || is_object($paramValue)) {
                $paramType = 'string'; // Will be JSON encoded
                $dbType = 'VARIANT';
            }
            
            $paramSchemas[strtolower($paramName)] = new ParameterSchema([
                'name' => $paramName,
                'position' => $position,
                'param_type' => 'IN',
                'type' => $paramType,
                'db_type' => $dbType,
                'length' => null,
                'precision' => null,
                'scale' => null,
            ]);
            
            $position++;
        }
        
        return $paramSchemas;
    }

    /**
     * @inheritdoc
     */
    public function typecastToClient($value, $field_info, $allow_null = true)
    {
        // Apply Snowflake-specific JSON encoding for function parameters
        if ($this->processingFunctionParameters && $field_info instanceof ParameterSchema) {
            $dbTypeUpper = strtoupper($field_info->dbType ?? '');
            if (($dbTypeUpper === 'ARRAY' || $dbTypeUpper === 'OBJECT' || $dbTypeUpper === 'VARIANT') 
                && (is_array($value) || is_object($value))) {
                return json_encode($value);
            } elseif (empty($field_info->dbType) && (is_array($value) || is_object($value))) {
                return json_encode($value);
            }
        }
        
        // Use parent implementation for all other cases
        return parent::typecastToClient($value, $field_info, $allow_null);
    }
}
