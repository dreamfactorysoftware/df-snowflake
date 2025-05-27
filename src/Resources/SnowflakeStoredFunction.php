<?php

namespace DreamFactory\Core\Snowflake\Resources;

use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;

class SnowflakeStoredFunction extends StoredFunction
{
    /**
     * @param string $name
     * @param bool $refresh
     * @return FunctionSchema
     * @throws \Exception
     */
    protected function getFunction($name, $refresh = false)
    {
        // Read database and schema from headers
        $databaseName = $this->request->getHeader('X-Database-Name');
        $schemaName = $this->request->getHeader('X-Schema-Name');

        // Build cache key including headers for proper caching
        $cacheKey = 'function:' . strtolower($name);
        if (!empty($databaseName)) {
            $cacheKey .= ':' . strtolower($databaseName);
        }
        if (!empty($schemaName)) {
            $cacheKey .= ':' . strtolower($schemaName);
        }

        // Try to get from cache first
        if (!$refresh && null !== ($cached = $this->parent->getFromCache($cacheKey))) {
            return $cached;
        }

        // If headers specify cross-database/schema access, create custom function schema
        if (!empty($databaseName) || !empty($schemaName)) {
            // Use current schema if not specified in header
            if (empty($schemaName)) {
                $schemaName = $this->parent->getSchema()->getDefaultSchema();
            }

            // Create function schema with header-specified database/schema
            $settings = [
                'name' => $name,
                'resourceName' => $name,
                'schemaName' => $schemaName,
            ];

            if (!empty($databaseName)) {
                $settings['databaseName'] = $databaseName;
                if (!empty($schemaName)) {
                    $settings['internalName'] = $databaseName . '.' . $schemaName . '.' . $name;
                    $settings['quotedName'] = $this->parent->getSchema()->quoteTableName($databaseName) . '.' . $this->parent->getSchema()->quoteTableName($schemaName) . '.' . $this->parent->getSchema()->quoteTableName($name);
                } else {
                    $settings['internalName'] = $databaseName . '.' . $name;
                    $settings['quotedName'] = $this->parent->getSchema()->quoteTableName($databaseName) . '.' . $this->parent->getSchema()->quoteTableName($name);
                }
            } else {
                $settings['internalName'] = $schemaName . '.' . $name;
                $settings['quotedName'] = $this->parent->getSchema()->quoteTableName($schemaName) . '.' . $this->parent->getSchema()->quoteTableName($name);
            }

            $function = new FunctionSchema($settings);
            
            // Load function parameters using the schema's getResource method
            $function = $this->parent->getSchema()->getResource(DbResourceTypes::TYPE_FUNCTION, $function);
            $function->discoveryCompleted = true;
            
            $this->parent->addToCache($cacheKey, $function, true);
            
            if (!$function) {
                throw new NotFoundException("Function '$name' does not exist in the specified database/schema.");
            }
            
            return $function;
        }

        // Fall back to parent implementation for standard function calls
        return parent::getFunction($name, $refresh);
    }
} 