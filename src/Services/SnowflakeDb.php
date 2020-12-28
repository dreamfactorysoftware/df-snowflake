<?php

namespace DreamFactory\Core\Snowflake\Services;

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

    public static function getDriverName()
    {
        return 'snowflake';
    }
}