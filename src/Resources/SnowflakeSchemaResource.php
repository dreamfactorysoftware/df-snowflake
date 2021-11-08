<?php
namespace DreamFactory\Core\Snowflake\Resources;

use DreamFactory\Core\Database\Resources\DbSchemaResource;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Utility\ResourcesWrapper;

class SnowflakeSchemaResource extends DbSchemaResource
{
    /**
     * {@inheritdoc}
     */
    public function createRelationship(
        $table,
        $relationship,
        $properties = [],
        $check_exist = false,
        $return_schema = false
    ) {
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $relationship;

        $fields = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $tables = [['name' => $table, 'related' => $fields]];
        $result = $this->updateSchema($tables, !$check_exist);
        $result = array_get($result, 0, []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeRelationship($table, $relationship);
        }

        return $result;
    }
}