<?php
namespace DreamFactory\Core\Snowflake\Resources;

use DB;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\BatchException;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Database\Schema\TableSchema;
use Illuminate\Support\Collection;

use Arr;

class SnowflakeTable extends Table
{
    /**
     * {@inheritdoc}
     */
    public static function getPrimaryKeys($avail_fields, $names_only = false)
    {
        $keys = [];
        foreach ($avail_fields as $info) {
            if ($info->isPrimaryKey || ($info->name === 'id' && $info->type === 'integer')) {
                $keys[] = ($names_only ? $info->name : $info);
            }
        }

        return $keys;
    }

    /**
     * {@inheritdoc}
     */
    public function createRecords($table, $records, $extras = [])
    {
        $records = static::validateAsArray($records, null, true, 'The request contains no valid record sets.');

        $isSingle = (1 == count($records));
        $fields = Arr::get($extras, ApiOptions::FIELDS);
        $idFields = Arr::get($extras, ApiOptions::ID_FIELD);
        $idTypes = Arr::get($extras, ApiOptions::ID_TYPE);
        $rollback = array_get_bool($extras, ApiOptions::ROLLBACK, false);
        $continue = array_get_bool($extras, ApiOptions::CONTINUES, false);
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes, false);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        $out = [];
        $errors = false;
        foreach ($records as $index => $record) {
            try {
                if (false === $id = $this->checkForIds($record, $this->tableIdsInfo, $extras, true)) {
                    throw new BadRequestException("Required id field(s) not found in record $index: " .
                        print_r($record, true));
                }

                $out[$index] = $this->addToTransaction($record, $id, $extras, $rollback, $continue, $isSingle);
            } catch (\Exception $ex) {
                $errors = true;
                $out[$index] = $ex;
                if ($rollback || !$continue) {
                    break;
                }
            }
        }

        if ($errors) {
            $msg = 'Batch Error: Not all requested records could be created.';

            if ($rollback) {
                $this->rollbackTransaction();
                $msg .= " All changes rolled back.";
            }

            throw new BatchException($out, $msg);
        }

        if ($result = $this->commitTransaction($extras)) {
            // operation performed, take output, override earlier
            $out = $result;
        }

        return $out;
    }
    /**
     * @param TableSchema $schema
     * @param Collection $result
     * @return array
     */
    public function decodeJsonField(TableSchema $schema, Collection $result): array {
        $columns = $schema->getColumns();
        $acceptedDbTypes = ["VARIANT"]; // Add your desired types here
        $nvcharColumns = [];
        foreach ($columns as $column) {
            if (!in_array($column->dbType, $acceptedDbTypes)) continue;
            $nvcharColumns[] = $column->name;
        }
        if (!empty($nvcharColumns)) {
            $temp = $result->map(function ($item) use ($nvcharColumns) {
                foreach ($nvcharColumns as $column) {
                    // json_decode wil return object if the decode is success or null
                    // in case of null => meaning the value is not valid json then we return the original value
                    $item[$column] = json_decode($item[$column]) ?? $item[$column];
                }
                return $item;
            });
            $result = collect($temp);
        }

        $data = $result->toArray();
        if (!empty($meta)) {
            $data['meta'] = $meta;
        }
        return $data;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
}