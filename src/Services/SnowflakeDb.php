<?php

namespace DreamFactory\Core\Snowflake\Services;

use DreamFactory\Core\Snowflake\Resources\SnowflakeTable as Table;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseRestResource;
use DreamFactory\Core\SqlDb\Services\SqlDb;

/**
 * Class SnowflakeDb
 *
 * @package DreamFactory\Core\Snowflake\Services
 */
class SnowflakeDb extends SqlDb
{
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $prefix = parent::getConfigBasedCachePrefix();
        $this->setConfigBasedCachePrefix($prefix);
    }

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'snowflake';
        parent::adaptConfig($config);
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();
        $paths = (array)array_get($base, 'paths');
        foreach ($paths as $pkey=>$path) {
            if (strpos($pkey, '_schema') !== false) {
                unset($paths[$pkey]);
                continue;
            }
            foreach ($path as $rkey=>$resource) {
                if ($rkey === 'post' || $rkey === 'patch' || $rkey === 'put' || $rkey === 'delete') {
                    unset($paths[$pkey][$rkey]);
                    continue;
                }
            }
        }
        $base['paths'] = $paths;

        return $base;
    }

    public static function getDriverName()
    {
        return 'snowflake';
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[Table::RESOURCE_NAME] = [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Table',
        ];

        return $handlers;
    }
}