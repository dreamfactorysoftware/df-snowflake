<?php

namespace DreamFactory\Core\Snowflake\Database\Schema;

use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;

class SnowflakeSchema extends SqlSchema
{
    const DEFAULT_SCHEMA = 'PUBLIC';

    /**
     * @inheritdoc
     */
    public function getDefaultSchema()
    {
        return static::DEFAULT_SCHEMA;
    }

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
            $c->isPrimaryKey = strpos($column['primary key'], 'Y') !== false;
            $c->isUnique = strpos($column['unique key'], 'Y') !== false;
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
                    $table->sequenceName = array_get($column, 'sequence', $c->name);
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
            $colName = array_get($row, 'column_name');
            $refColName = array_get($row, 'referenced_column_name');
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