<?php

namespace DreamFactory\Core\Snowflake\Database\Schema;

use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use Arr;

class SnowflakeSchema extends SqlSchema
{
    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
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

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames($schema = '')
    {
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

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineParamString(array $param_schemas, array &$values)
    {
        $paramStr = '';
        foreach ($param_schemas as $key => $paramSchema) {
            if (!empty($values[strtolower($paramSchema->name)])) {
                $pName = ':' . $paramSchema->name;
                $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
            }
        }

        return $paramStr;
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineNames($type, $schema = '')
    {
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
                ('PROCEDURE' === $type) ? new ProcedureSchema($settings) : new FunctionSchema($settings);
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


        $sql = <<<MYSQL
SELECT * FROM INFORMATION_SCHEMA.{$type}S WHERE {$type}_NAME = '{$holder->resourceName}' AND {$type}_SCHEMA = '{$holder->schemaName}'
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
        $values = $this->determineRoutineValues($paramSchemas, $in_params);

        $sql = $this->getFunctionStatement($function, $paramSchemas, $values);
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

    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        if (is_array($schema)) {
            $schema = implode("','", $schema);
        }

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

}