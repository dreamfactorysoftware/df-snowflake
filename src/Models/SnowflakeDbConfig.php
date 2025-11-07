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
            // Step 1: ODBC Toggle (affects everything)
            [
                'name' => 'use_odbc',
                'label' => 'Use ODBC Driver',
                'type' => 'boolean',
                'description' => 'Enable to use ODBC driver instead of PDO. Required for OAuth authentication.'
            ],

            // Step 2: Authentication Method Selector
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

            // Step 3a: Username/Password Authentication Fields
            [
                'name' => 'username',
                'label' => 'Username',
                'type' => 'string',
                'description' => 'The name of the Snowflake account user. This can be a lookup key.'
            ],
            [
                'name' => 'password',
                'label' => 'Password',
                'type' => 'password',
                'description' => 'The password for the Snowflake account user. This can be a lookup key. Required for Username/Password authentication.'
            ],

            // Step 3b: Key Pair (JWT) Authentication Fields
            [
                'name' => 'key',
                'label' => 'Private Key File',
                'type' => 'file_certificate_api',
                'description' => 'Specifies the path to the private key file for key pair authentication. ' .
                    'When using key pair authentication, select an existing key file from a file service or upload a new one. ' .
                    'For information on creating key pairs, see <a href="https://docs.snowflake.com/en/user-guide/key-pair-auth" target="_blank">Snowflake Key Pair Authentication</a>.'
            ],
            [
                'name' => 'passcode',
                'label' => 'Private Key Passphrase',
                'type' => 'password',
                'description' => 'If your private key file is encrypted, specify the passphrase here. Leave blank if your private key is not encrypted.'
            ],

            // Step 3c: OAuth Authentication Fields
            [
                'name' => 'oauth_client_id',
                'label' => 'OAuth Client ID',
                'type' => 'string',
                'description' => 'OAuth 2.0 Client ID for OAuth authentication. Required when using OAuth.'
            ],
            [
                'name' => 'oauth_client_secret_raw',
                'label' => 'OAuth Client Secret',
                'type' => 'password',
                'description' => 'OAuth 2.0 Client Secret for OAuth authentication. Required when using OAuth.'
            ],

            // Step 4: Connection Details (always visible)
            [
                'name' => 'database',
                'label' => 'Database',
                'type' => 'string',
                'description' => 'The name of the database to connect to on the given server. This can be a lookup key.'
            ],
            [
                'name' => 'schema',
                'label' => 'Schema',
                'type' => 'string',
                'description' => 'Leave blank to work with the default schema or type in a specific schema to use for this service.'
            ],
            [
                'name' => 'role',
                'label' => 'Role',
                'type' => 'string',
                'description' => 'User\'s role to use for this connection.'
            ],
            [
                'name' => 'warehouse',
                'label' => 'Warehouse',
                'type' => 'string',
                'description' => 'The name of the warehouse your database uses.'
            ],

            // Advanced Settings (move to separate section)
            [
                'name' => 'hostname',
                'label' => 'Hostname (Advanced)',
                'type' => 'string',
                'description' => 'Snowflake hostname. This can be an alternative Snowflake hostname (Optional). Leave blank to use default based on account.'
            ],
            [
                'name' => 'account',
                'label' => 'Account (Advanced)',
                'type' => 'string',
                'description' => 'Your Snowflake account identifier (e.g., UCZWIRU-JUB93638). This is used for ODBC connections. (<a href="https://docs.snowflake.com/en/user-guide/connecting.html#your-snowflake-account-name" target="_blank">doc</a>)'
            ],
            [
                'name' => 'account_locator',
                'label' => 'Account Locator (Advanced)',
                'type' => 'string',
                'description' => 'Your Snowflake account locator (e.g., njb13282). Required only for OAuth authentication. Leave blank for non-OAuth connections.'
            ],

            // OAuth Token Fields (read-only, auto-populated)
            [
                'name' => 'oauth_access_token',
                'label' => 'OAuth Access Token (Auto-populated)',
                'type' => 'password',
                'description' => 'Current OAuth access token (auto-populated after authorization via the OAuth panel below).'
            ],
            [
                'name' => 'oauth_refresh_token',
                'label' => 'OAuth Refresh Token (Auto-populated)',
                'type' => 'password',
                'description' => 'OAuth refresh token (auto-populated after authorization).'
            ],
            [
                'name' => 'oauth_token_expires_at',
                'label' => 'OAuth Token Expiration (Auto-populated)',
                'type' => 'string',
                'description' => 'Token expiration timestamp (auto-populated).'
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

    /**
     * {@inheritdoc}
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'hostname':
            case 'account':
            case 'account_locator':
            case 'username':
            case 'password':
            case 'key':
            case 'passcode':
            case 'database':
            case 'warehouse':
            case 'schema':
            case 'role':
            case 'authenticator':
            case 'oauth_client_id':
            case 'oauth_client_secret_raw':
            case 'oauth_access_token':
            case 'oauth_refresh_token':
            case 'oauth_token_expires_at':
            case 'use_odbc':
                $schema['description'] = array_get($schema, 'description', '');
                break;
        }
    }

    /**
     * Apply native app auto-configuration if requested
     *
     * @param array $record
     * @return array
     */
    protected static function beforeSave(&$record)
    {
        // Check if auto-configuration is requested
        if (isset($record['auto_configure_native_app']) && $record['auto_configure_native_app']) {
            \Log::info('Auto-configuring Snowflake service from native app environment');

            // Import the utility class
            $nativeAppUtility = 'DreamFactory\Core\Snowflake\Utility\SnowflakeNativeAppDetector';

            if (class_exists($nativeAppUtility)) {
                // Merge native app config with user-provided values
                // User-provided values take precedence
                $record = call_user_func([$nativeAppUtility, 'mergeWithUserConfig'], $record);

                \Log::info('Native app auto-configuration applied', [
                    'has_account' => !empty($record['account']),
                    'has_oauth_client_id' => !empty($record['oauth_client_id']),
                    'has_database' => !empty($record['database'])
                ]);
            }

            // Remove the auto-configure flag so it doesn't get saved
            unset($record['auto_configure_native_app']);
        }

        return $record;
    }
}