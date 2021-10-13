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
        foreach ($paths as $pkey => $path) {
            foreach ($path as $rkey => $resource) {
                if ($rkey === 'patch' || $rkey === 'put') {
                    unset($paths[$pkey][$rkey]);
                    continue;
                }
            }
        }
        foreach ($paths as $pkey => $path) {
            if ($pkey !== '/' && isset($path['get']) && isset($path['get']['parameters'])) {
                $newParams = [
                    $this->getHeaderPram('account'),
                    $this->getHeaderPram('username'),
                    $this->getHeaderPram('password'),
                    $this->getHeaderPram('role'),
                    $this->getHeaderPram('database'),
                    $this->getHeaderPram('warehouse'),
                    $this->getHeaderPram('schema')
                ];
                $paths[$pkey]['get']['parameters'] = array_merge($paths[$pkey]['get']['parameters'], $newParams);
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
            'name' => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label' => 'Table',
        ];

        return $handlers;
    }

    private function getHeaderPram($name): array
    {
        return [
            "name" => $name,
            "description" => ucfirst($name) . " for database connection.",
            "schema" => [
                "type" => "string"
            ],
            "in" => "header",
            "required" => false
        ];
    }
}