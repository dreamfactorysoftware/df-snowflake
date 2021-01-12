<?php
namespace DreamFactory\Core\Snowflake\Resources;

use DB;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Components\Expression;
use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\BatchException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;

class SnowflakeTable extends BaseDbTableResource
{
    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        return [];
    }

    /**
     * @param              $table
     * @param Builder      $builder
     * @param array        $extras
     * @return int|array
     * @throws BadRequestException
     * @throws InternalServerErrorException
     * @throws NotFoundException
     * @throws RestException
     */
    protected function runQuery($table, Builder $builder, $extras)
    {
        return [];
    }

    /**
     * @param TableSchema $schema
     * @param Builder     $builder
     * @param array       $extras
     * @return Collection
     */
    protected function getQueryResults(TableSchema $schema, Builder $builder, $extras)
    {
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array|\DreamFactory\Core\Database\Schema\ColumnSchema[]
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        return [];
    }

    /**
     * @param array   $avail_fields
     * @param boolean $names_only Return only an array of names, otherwise return all properties
     *
     * @return array
     */
    public static function getPrimaryKeys($avail_fields, $names_only = false)
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ) {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
}