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
    protected $appends = ['hostname', 'account', 'account_locator', 'username', 'password', 'key', 'passcode', 'database', 'warehouse', 'schema', 'role', 'authenticator', 'oauth_client_id', 'oauth_client_secret', 'oauth_client_secret_raw', 'oauth_access_token', 'oauth_refresh_token', 'oauth_token_expires_at', 'use_odbc'];

    // WORKAROUND: Remove oauth tokens from encryption due to truncation bug in df-core
    protected $encrypted = ['username', 'password', 'key', 'passcode', 'oauth_client_secret'];

    // WORKAROUND: OAuth tokens NOT protected to avoid truncation (they're also not encrypted above)
    protected $protected = ['password', 'oauth_client_secret'];

    protected function getConnectionFields()
    {
        return ['hostname', 'account', 'account_locator', 'username', 'password', 'key', 'passcode', 'database', 'warehouse', 'schema', 'role', 'authenticator', 'oauth_client_id', 'oauth_client_secret', 'oauth_client_secret_raw', 'oauth_access_token', 'oauth_refresh_token', 'oauth_token_expires_at', 'use_odbc'];
    }

    public static function getDriverName()
    {
        return 'snowflake';
    }


    public static function getDefaultConnectionInfo()
    {
        $defaults = [
            [
                'name' => 'use_odbc',
                'label' => 'Use ODBC Driver',
                'type' => 'boolean',
                'description' => 'Enable to use ODBC driver instead of PDO. Required for OAuth authentication.'
            ],
            [
                'name' => 'authenticator',
                'label' => 'Authentication Method',
                'type' => 'picklist',
                'values' => [
                    ['label' => 'Username/Password', 'name' => 'snowflake'],
                    ['label' => 'Key Pair (JWT)', 'name' => 'snowflake_jwt'],
                    ['label' => 'OAuth', 'name' => 'oauth'],
                    ['label' => 'External Browser SSO', 'name' => 'externalbrowser']
                ],
                'description' => 'Choose the authentication method for connecting to Snowflake.'
            ],
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
                'description' => 'Your Snowflake account identifier (e.g., UCZWIRU-JUB93638). This is used for ODBC connections (<a href="https://docs.snowflake.com/en/user-guide/connecting.html#your-snowflake-account-name">doc</a>).'
            ],
            [
                'name' => 'account_locator',
                'label' => 'Account Locator',
                'type' => 'string',
                'description' => 'Your Snowflake account locator (e.g., njb13282). Required only for OAuth authentication. Leave blank for non-OAuth connections.'
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
                    'If you are using key pair or OAuth authentication, leave this blank.'
            ],
            [
                'name' => 'passcode',
                'label' => 'Private Key Passphrase',
                'type' => 'password',
                'description' => 'If your private key file is encrypted, specify the passphrase here. ' .
                    'Leave blank if your private key is not encrypted.'
            ],
            [
                'name' => 'oauth_client_id',
                'label' => 'OAuth Client ID',
                'type' => 'string',
                'description' => 'OAuth 2.0 Client ID for OAuth authentication.'
            ],
            [
                'name' => 'oauth_client_secret',
                'label' => 'OAuth Client Secret (Deprecated)',
                'type' => 'password',
                'description' => 'DEPRECATED: Use oauth_client_secret_raw instead. This field has a truncation bug.'
            ],
            [
                'name' => 'oauth_client_secret_raw',
                'label' => 'OAuth Client Secret',
                'type' => 'password',
                'description' => 'OAuth 2.0 Client Secret for OAuth authentication (unencrypted workaround for truncation bug).'
            ],
            [
                'name' => 'oauth_access_token',
                'label' => 'OAuth Access Token',
                'type' => 'password',
                'description' => 'Current OAuth access token (auto-populated after authorization).'
            ],
            [
                'name' => 'oauth_refresh_token',
                'label' => 'OAuth Refresh Token',
                'type' => 'password',
                'description' => 'OAuth refresh token (auto-populated after authorization).'
            ],
            [
                'name' => 'oauth_token_expires_at',
                'label' => 'OAuth Token Expiration',
                'type' => 'string',
                'description' => 'Token expiration timestamp (auto-populated).'
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