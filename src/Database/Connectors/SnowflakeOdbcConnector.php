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
        $dsn = $this->getDsn($config);

        // ODBC connections don't use username/password in odbc_connect for OAuth
        // They're passed via DSN or connection attributes
        $username = $config['username'] ?? '';
        $password = $config['password'] ?? '';

        // Override password if using OAuth
        if (!empty($config['oauth_access_token'])) {
            $password = '';
        }

        try {
            $connection = odbc_connect($dsn, $username, $password);

            if (!$connection) {
                throw new \Exception('Failed to connect to Snowflake via ODBC: ' . odbc_errormsg());
            }

            // Set connection attributes
            $this->configureConnection($connection, $config);

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

        // Add server/account information
        if (!empty($hostname)) {
            $dsn .= "Server={$hostname};";
        }

        if (!empty($account)) {
            $dsn .= "Account={$account};";
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
                    $token = $this->escapeDsnValue($config['oauth_access_token']);
                    $dsn .= "Token={$token};";
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
