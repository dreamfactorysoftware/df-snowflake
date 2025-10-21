<?php

namespace DreamFactory\Core\Snowflake\Database\Connectors;

use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

/**
 * Snowflake ODBC Connector
 *
 * Provides ODBC-based connectivity to Snowflake with support for:
 * - Username/Password authentication
 * - Key-pair (JWT) authentication
 * - OAuth 2.0 authentication
 * - External browser SSO authentication
 */
class SnowflakeOdbcConnector extends Connector implements ConnectorInterface
{
    /**
     * Establish a database connection using ODBC
     *
     * @param  array  $config
     * @return resource ODBC connection resource
     */
    public function connect(array $config)
    {
        // Debug: Log config structure
        \Log::info('SnowflakeOdbcConnector::connect() called with config', [
            'config_keys' => array_keys($config),
            'has_oauth_access_token' => isset($config['oauth_access_token']),
            'oauth_token_length' => isset($config['oauth_access_token']) ? strlen($config['oauth_access_token']) : 0,
            'authenticator' => $config['authenticator'] ?? 'not set',
            'username' => $config['username'] ?? 'not set',
        ]);

        $dsn = $this->getDsn($config);

        // ODBC connections don't use username/password in odbc_connect for OAuth
        // They're passed via DSN or connection attributes
        $username = $config['username'] ?? '';
        $password = $config['password'] ?? '';

        // Override username/password if using OAuth - token is in DSN
        if (!empty($config['oauth_access_token'])) {
            $username = '';
            $password = '';
        }

        try {
            // Suppress PHP warnings during connection attempt
            $connection = @odbc_connect($dsn, $username, $password);

            if (!$connection) {
                // Get detailed error information
                $errorMsg = 'Failed to connect to Snowflake via ODBC';

                // Try to get ODBC error message
                if (function_exists('odbc_errormsg')) {
                    $odbcError = odbc_errormsg();
                    if (!empty($odbcError)) {
                        $errorMsg .= ': ' . $odbcError;
                    }
                }

                // Also check for error code
                if (function_exists('odbc_error')) {
                    $errorCode = odbc_error();
                    if (!empty($errorCode)) {
                        $errorMsg .= ' (Error code: ' . $errorCode . ')';
                    }
                }

                \Log::error('Snowflake ODBC connection failed', [
                    'dsn' => $this->sanitizeDsnForLogging($dsn),
                    'username' => $username,
                    'error' => $errorMsg
                ]);

                throw new \Exception($errorMsg);
            }

            // Set connection attributes
            $this->configureConnection($connection, $config);

            \Log::info('Snowflake ODBC connection successful');
            return $connection;
        } catch (\Exception $e) {
            \Log::error('Snowflake ODBC connection error: ' . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Configure ODBC connection with additional settings
     *
     * @param  resource  $connection
     * @param  array  $config
     * @return void
     */
    protected function configureConnection($connection, array $config)
    {
        // Set autocommit mode
        if (function_exists('odbc_autocommit')) {
            odbc_autocommit($connection, true);
        }

        // Additional connection settings can be configured here
        // Note: ODBC doesn't support setAttribute like PDO
    }

    /**
     * Create a DSN string from configuration
     *
     * @param  array  $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        // Start with the ODBC driver name
        $dsn = "Driver=Snowflake;";

        // ========================================
        // COMPREHENSIVE LOGGING FOR SPCS ENV VARS
        // ========================================
        \Log::info('=== CHECKING FOR SPCS ENVIRONMENT VARIABLES ===');

        // Check for SPCS environment variables (when running in Snowpark Container Services)
        $spcsHost = getenv('SNOWFLAKE_HOST');
        $spcsAccount = getenv('SNOWFLAKE_ACCOUNT');

        \Log::info('SNOWFLAKE_HOST environment variable', [
            'exists' => $spcsHost !== false,
            'value' => $spcsHost ?: 'NOT SET',
            'is_empty' => empty($spcsHost),
            'type' => gettype($spcsHost)
        ]);

        \Log::info('SNOWFLAKE_ACCOUNT environment variable', [
            'exists' => $spcsAccount !== false,
            'value' => $spcsAccount ?: 'NOT SET',
            'is_empty' => empty($spcsAccount),
            'type' => gettype($spcsAccount)
        ]);

        // Log ALL environment variables starting with SNOWFLAKE
        $allEnv = getenv();
        $snowflakeEnvVars = array_filter($allEnv, function($key) {
            return strpos(strtoupper($key), 'SNOWFLAKE') === 0;
        }, ARRAY_FILTER_USE_KEY);

        \Log::info('All SNOWFLAKE_* environment variables found', [
            'count' => count($snowflakeEnvVars),
            'vars' => $snowflakeEnvVars
        ]);

        // Also check for common container/service env vars
        $containerEnvVars = [
            'HOSTNAME' => getenv('HOSTNAME'),
            'PWD' => getenv('PWD'),
            'HOME' => getenv('HOME'),
        ];
        \Log::info('Container environment context', $containerEnvVars);

        if ($spcsHost && $spcsAccount) {
            \Log::info('✅ SPCS environment detected! Using SNOWFLAKE_HOST and SNOWFLAKE_ACCOUNT', [
                'host' => $spcsHost,
                'account' => $spcsAccount
            ]);
        } else {
            \Log::warning('❌ SPCS environment variables NOT found or empty!', [
                'SNOWFLAKE_HOST_exists' => $spcsHost !== false,
                'SNOWFLAKE_ACCOUNT_exists' => $spcsAccount !== false,
                'will_use_config_values' => true
            ]);
        }

        // Add server/account information
        // Priority: SPCS env vars > account_locator > account > hostname
        $accountForServer = $spcsAccount ?: ($config['account'] ?? null);
        $serverHostname = $spcsHost ?: ($config['hostname'] ?? null);

        \Log::info('Final account/hostname selection', [
            'accountForServer' => $accountForServer,
            'serverHostname' => $serverHostname,
            'source_account' => $spcsAccount ? 'SPCS env' : 'config',
            'source_hostname' => $spcsHost ? 'SPCS env' : 'config'
        ]);

        if ($authenticator === 'oauth' && !empty($config['account_locator']) && !$spcsAccount) {
            $accountForServer = strtolower($config['account_locator']);
            \Log::info('Using account_locator for OAuth', ['account' => $accountForServer]);
        }

        // ODBC requires Server parameter - use SPCS host if available, otherwise construct
        if (!empty($serverHostname)) {
            $dsn .= "Server={$serverHostname};";
        } elseif (!empty($accountForServer)) {
            // Construct server URL from account name
            $server = "{$accountForServer}.snowflakecomputing.com";
            $dsn .= "Server={$server};";
        } else {
            throw new \InvalidArgumentException("Either hostname or account is required for Snowflake connections.");
        }

        // Account is still needed for some operations
        if (!empty($accountForServer)) {
            $dsn .= "Account={$accountForServer};";
        }

        // Add database context
        if (!empty($database)) {
            $dsn .= "Database={$database};";
        }

        if (!empty($schema)) {
            $dsn .= "Schema={$schema};";
        } else {
            throw new \InvalidArgumentException("Schema is required for Snowflake connections.");
        }

        if (!empty($warehouse)) {
            $dsn .= "Warehouse={$warehouse};";
        }

        if (!empty($role)) {
            $dsn .= "Role={$role};";
        }

        // Determine authentication method
        $authenticator = $config['authenticator'] ?? 'snowflake';

        switch ($authenticator) {
            case 'oauth':
                // OAuth token-based authentication
                $dsn .= "Authenticator=oauth;";
                if (!empty($config['oauth_access_token'])) {
                    // Do NOT escape the token - it should be passed as-is
                    // Also add UID parameter with username for OAuth
                    $token = $config['oauth_access_token'];
                    \Log::info('OAuth token found in config', [
                        'token_length' => strlen($token),
                        'token_preview' => substr($token, 0, 20) . '...' . substr($token, -20),
                        'username' => $config['username'] ?? 'not set'
                    ]);
                    $dsn .= "Token={$token};";

                    // Add username as UID for OAuth authentication
                    if (!empty($config['username'])) {
                        $uid = $this->escapeDsnValue($config['username']);
                        $dsn .= "UID={$uid};";
                    }
                } else {
                    \Log::warning('OAuth authenticator selected but oauth_access_token is empty!', [
                        'config_keys' => array_keys($config)
                    ]);
                }
                break;

            case 'externalbrowser':
                // External browser SSO authentication
                $dsn .= "Authenticator=externalbrowser;";
                break;

            case 'snowflake_jwt':
                // Key-pair authentication
                $dsn .= "Authenticator=SNOWFLAKE_JWT;";

                if (!empty($key)) {
                    $keyPath = realpath($key);
                    if (!$keyPath || !file_exists($keyPath)) {
                        throw new \InvalidArgumentException("Private key file not found at: {$key}");
                    }

                    $escapedKeyPath = $this->escapeDsnValue($keyPath);
                    $dsn .= "priv_key_file={$escapedKeyPath};";

                    if (!empty($passcode)) {
                        $escapedPasscode = $this->escapeDsnValue($passcode);
                        $dsn .= "priv_key_file_pwd={$escapedPasscode};";
                    }
                }
                break;

            case 'snowflake':
            default:
                // Standard username/password authentication
                $dsn .= "Authenticator=snowflake;";
                break;
        }

        // Add application identifier
        $dsn .= "Application=DreamFactory_Snowflake_ODBC;";

        // Memory optimization settings for ODBC driver
        // Use client-side result set to reduce server-side buffering
        $dsn .= "CLIENT_RESULT_CHUNK_SIZE=16;"; // Smaller chunks (default is 128)
        $dsn .= "CLIENT_PREFETCH_THREADS=1;"; // Reduce prefetch threads

        \Log::debug('Snowflake ODBC DSN (sanitized): ' . $this->sanitizeDsnForLogging($dsn));

        return $dsn;
    }

    /**
     * Escape special characters in DSN values
     *
     * @param  string  $value
     * @return string
     */
    protected function escapeDsnValue($value)
    {
        // Escape semicolons and equals signs which have special meaning in DSN strings
        return str_replace([';', '='], ['\\;', '\\='], $value);
    }

    /**
     * Sanitize DSN string for logging (remove sensitive data)
     *
     * @param  string  $dsn
     * @return string
     */
    protected function sanitizeDsnForLogging($dsn)
    {
        // Remove sensitive values from log output
        $sanitized = preg_replace('/Token=([^;]+);/', 'Token=***;', $dsn);
        $sanitized = preg_replace('/priv_key_file_pwd=([^;]+);/', 'priv_key_file_pwd=***;', $sanitized);
        return $sanitized;
    }
}
