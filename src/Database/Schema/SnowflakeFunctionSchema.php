<?php

namespace DreamFactory\Core\Snowflake\Database\Schema;

use DreamFactory\Core\Database\Schema\FunctionSchema;

/**
 * SnowflakeFunctionSchema extends the base FunctionSchema to add Snowflake-specific properties
 */
class SnowflakeFunctionSchema extends FunctionSchema
{
    /**
     * @var string Specific database name for this routine, if needed different from connection default.
     */
    public $databaseName = null;
    
    /**
     * @var string Original database type for the return value of this routine.
     */
    public $returnDbtype = null;
} 