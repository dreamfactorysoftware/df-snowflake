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
            // BASIC FIELDS (for Snowflake Native App users)
            // Step 1: ODBC Toggle (affects everything)
            [
                'name' => 'use_odbc',
                'label' => 'Use ODBC Driver',
                'type' => 'boolean',
                'description' => 'Enable to use ODBC driver instead of PDO. Required for OAuth authentication.',
                'category' => 'basic'
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
                'description' => 'Choose the authentication method for connecting to Snowflake.',
                'category' => 'basic'
            ],

            // Step 3: Account Locator (Primary field for OAuth in Native App)
            [
                'name' => 'account_locator',
                'label' => 'Account Locator',
                'type' => 'string',
                'description' => 'Your Snowflake account locator (e.g., njb13282). Required for OAuth authentication.',
                'category' => 'basic'
            ],

            // Step 4: Connection Details (visible in basic view)
            [
                'name' => 'database',
                'label' => 'Database',
                'type' => 'string',
                'description' => 'The name of the database to connect to on the given server. This can be a lookup key.',
                'category' => 'basic'
            ],
            [
                'name' => 'warehouse',
                'label' => 'Warehouse',
                'type' => 'string',
                'description' => 'The name of the warehouse your database uses.',
                'category' => 'basic'
            ],
            [
                'name' => 'schema',
                'label' => 'Schema',
                'type' => 'string',
                'description' => 'Leave blank to work with the default schema or type in a specific schema to use for this service.',
                'category' => 'basic'
            ],
            [
                'name' => 'role',
                'label' => 'Role',
                'type' => 'string',
                'description' => 'User\'s role to use for this connection.',
                'category' => 'basic'
            ],

            // ADVANCED FIELDS (for advanced users and non-Native App scenarios)
            // Alternative Authentication: Username/Password
            [
                'name' => 'username',
                'label' => 'Username',
                'type' => 'string',
                'description' => 'The name of the Snowflake account user. This can be a lookup key. Required for Username/Password authentication.',
                'category' => 'advanced'
            ],
            [
                'name' => 'password',
                'label' => 'Password',
                'type' => 'password',
                'description' => 'The password for the Snowflake account user. This can be a lookup key. Required for Username/Password authentication.',
                'category' => 'advanced'
            ],

            // Alternative Authentication: Key Pair (JWT)
            [
                'name' => 'key',
                'label' => 'Private Key File',
                'type' => 'file_certificate_api',
                'description' => 'Specifies the path to the private key file for key pair authentication. ' .
                    'When using key pair authentication, select an existing key file from a file service or upload a new one. ' .
                    'For information on creating key pairs, see <a href="https://docs.snowflake.com/en/user-guide/key-pair-auth" target="_blank">Snowflake Key Pair Authentication</a>.',
                'category' => 'advanced'
            ],
            [
                'name' => 'passcode',
                'label' => 'Private Key Passphrase',
                'type' => 'password',
                'description' => 'If your private key file is encrypted, specify the passphrase here. Leave blank if your private key is not encrypted.',
                'category' => 'advanced'
            ],

            // OAuth Configuration (not needed in Native App - system handles this)
            [
                'name' => 'oauth_client_id',
                'label' => 'OAuth Client ID',
                'type' => 'string',
                'description' => 'OAuth 2.0 Client ID for OAuth authentication. Not required for Snowflake Native App (auto-configured).',
                'category' => 'advanced'
            ],
            [
                'name' => 'oauth_client_secret_raw',
                'label' => 'OAuth Client Secret',
                'type' => 'password',
                'description' => 'OAuth 2.0 Client Secret for OAuth authentication. Not required for Snowflake Native App (auto-configured).',
                'category' => 'advanced'
            ],

            // Advanced Connection Settings
            [
                'name' => 'hostname',
                'label' => 'Hostname',
                'type' => 'string',
                'description' => 'Snowflake hostname. This can be an alternative Snowflake hostname (Optional). Leave blank to use default based on account.',
                'category' => 'advanced'
            ],
            [
                'name' => 'account',
                'label' => 'Account',
                'type' => 'string',
                'description' => 'Your Snowflake account identifier (e.g., UCZWIRU-JUB93638). This is used for ODBC connections. (<a href="https://docs.snowflake.com/en/user-guide/connecting.html#your-snowflake-account-name" target="_blank">doc</a>)',
                'category' => 'advanced'
            ],

            // OAuth Token Fields (read-only, auto-populated, hidden in basic view)
            [
                'name' => 'oauth_access_token',
                'label' => 'OAuth Access Token (Auto-populated)',
                'type' => 'password',
                'description' => 'Current OAuth access token (auto-populated after authorization via the OAuth panel below).',
                'category' => 'advanced'
            ],
            [
                'name' => 'oauth_refresh_token',
                'label' => 'OAuth Refresh Token (Auto-populated)',
                'type' => 'password',
                'description' => 'OAuth refresh token (auto-populated after authorization).',
                'category' => 'advanced'
            ],
            [
                'name' => 'oauth_token_expires_at',
                'label' => 'OAuth Token Expiration (Auto-populated)',
                'type' => 'string',
                'description' => 'Token expiration timestamp (auto-populated).',
                'category' => 'advanced'
            ]
        ];
        return $defaults;
    }

    /** {@inheritdoc} */
    public static function getConfigSchema()
    {
        $model = new static;

        $schema = $model->getTableSchema();
        if ($schema) {
            $out = [];
            foreach ($schema->columns as $name => $column) {
                if ('connection' === $name) {
                    // specific attributes to the different databases
                    $connectionInfo = static::getDefaultConnectionInfo();
                    // Process each connection field through prepareConfigSchemaField
                    foreach ($connectionInfo as &$field) {
                        static::prepareConfigSchemaField($field);
                    }
                    $out = array_merge($out, $connectionInfo);
                }

                // Skip if column is hidden
                if (in_array($name, $model->getHidden())) {
                    continue;
                }
                /** @var \DreamFactory\Core\Database\Schema\ColumnSchema $column */
                if (('service_id' === $name) || $column->autoIncrement) {
                    continue;
                }

                $temp = $column->toArray();
                static::prepareConfigSchemaField($temp);
                $out[] = $temp;
            }

            // Remove options, attributes, and statements (Snowflake doesn't use these)
            $out = array_filter($out, function($field) {
                return !in_array($field['name'], ['options', 'attributes', 'statements']);
            });

            // Add allow upsert here
            $out = array_merge($out, static::getExtraConfigSchema(), \DreamFactory\Core\Models\ServiceCacheConfig::getConfigSchema());

            return $out;
        }

        return null;
    }

    /**
     * {@inheritdoc}
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        // The 'category' property is already set in getDefaultConnectionInfo()
        // This method is called by getConfigSchema() to preserve it
        // No additional processing needed - category will be preserved
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