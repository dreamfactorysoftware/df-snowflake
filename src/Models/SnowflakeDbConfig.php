<?php

namespace DreamFactory\Core\Snowflake\Models;

use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\SqlDb\Models\BaseSqlDbConfig;

/**
 * SnowflakeDbConfig
 *
 */
class SnowflakeDbConfig extends BaseSqlDbConfig
{
    protected $appends = ['hostname', 'account', 'username', 'password', 'key', 'passcode', 'database', 'warehouse', 'schema', 'role'];

    protected $encrypted = ['username', 'password', 'key', 'passcode'];

    protected $protected = ['password'];

    protected function getConnectionFields()
    {
        return ['hostname', 'account', 'username', 'password', 'key', 'passcode', 'database', 'warehouse', 'schema', 'role'];
    }

    public static function getDriverName()
    {
        return 'snowflake';
    }


    public static function getDefaultConnectionInfo()
    {
        $defaults = [
            [
                'name' => 'hostname',
                'label' => 'Hostname',
                'type' => 'string',
                'description' => 'Snowflake hostname, This can be alternative snowflake hostname (Optional).'
            ],
            [
                'name' => 'account',
                'label' => 'Account',
                'type' => 'string',
                'description' => 'Your Snowflake account name (<a href="https://docs.snowflake.com/en/user-guide/connecting.html#your-snowflake-account-name">doc</a>).'
            ],
            [
                'name' => 'username',
                'label' => 'Username',
                'type' => 'string',
                'description' => 'The name of the snowflake account user. This can be a lookup key.'
            ],
            [
                'name' => 'key',
                'label' => 'Private Key File',
                'type' => 'file_certificate_api',
                'description' => 'Specifies the path to the private key file for key pair authentication. ' .
                    'When using key pair authentication, select an existing key file from a file service or upload a new one. ' .
                    'For information on creating key pairs, see <a href="https://docs.snowflake.com/en/user-guide/key-pair-auth" target="_blank">Snowflake Key Pair Authentication</a>.'
            ],
            [
                'name' => 'password',
                'label' => 'Password',
                'type' => 'password',
                'description' => 'The password for the snowflake account user. This can be a lookup key. ' .
                    'If you are using key pair authentication, leave this blank.'
            ],
            [
                'name' => 'passcode',
                'label' => 'Private Key Passphrase',
                'type' => 'password',
                'description' => 'If your private key file is encrypted, specify the passphrase here. ' .
                    'Leave blank if your private key is not encrypted.'
            ],
            [
                'name' => 'role',
                'label' => 'Role',
                'type' => 'string',
                'description' => 'User\'s role.'
            ],
            [
                'name' => 'database',
                'label' => 'Database',
                'type' => 'string',
                'description' => 'The name of the database to connect to on the given server. This can be a lookup key.'
            ],
            [
                'name' => 'warehouse',
                'label' => 'Warehouse',
                'type' => 'string',
                'description' => 'The name of the warehouse your database uses.'
            ],
            [
                'name' => 'schema',
                'label' => 'Schema',
                'type' => 'string',
                'description' => 'Leave blank to work with the default schema ' .
                    'or type in a specific schema to use for this service.'
            ]
        ];
        return $defaults;
    }

    /** {@inheritdoc} */
    public static function getConfigSchema()
    {
        $schema = parent::getConfigSchema();
        $cacheTtl = array_pop($schema);
        $cacheEnabled = array_pop($schema);
        $maxRecords = array_pop($schema);
        $upserts = array_pop($schema);
        array_pop($schema);                 // Remove statement
        array_pop($schema);                 // Remove attributes
        array_pop($schema);                 // Remove options
        array_push($schema, $upserts);      // Restore upsert
        array_push($schema, $maxRecords);   // Restore max_records
        array_push($schema, $cacheEnabled); // Restore cache enabled
        array_push($schema, $cacheTtl);     // Restore cache TTL

        return $schema;
    }
}