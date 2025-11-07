<?php

namespace DreamFactory\Core\Snowflake\Utility;

/**
 * Utility class to detect and configure Snowflake Native App environment
 */
class SnowflakeNativeAppDetector
{
    /**
     * Detect if running inside Snowflake Native App
     *
     * @return bool
     */
    public static function isNativeApp(): bool
    {
        // Check for Snowflake-specific environment variables
        return !empty(env('SNOWFLAKE_ACCOUNT'))
            || !empty(env('SNOWFLAKE_WAREHOUSE'))
            || !empty(env('SNOWFLAKE_DATABASE'))
            || !empty(env('SNOWFLAKE_HOST'));
    }

    /**
     * Get native app configuration from environment
     *
     * @return array
     */
    public static function getNativeAppConfig(): array
    {
        if (!self::isNativeApp()) {
            return [];
        }

        $account = env('SNOWFLAKE_ACCOUNT', '');
        $hostname = env('SNOWFLAKE_HOST', '');

        // Extract account locator from full account identifier if not explicitly set
        // UCZWIRU-JUB93638 -> jub93638 (account locator format)
        $accountLocator = env('SNOWFLAKE_ACCOUNT_LOCATOR', '');
        if (empty($accountLocator) && !empty($account)) {
            $accountParts = explode('-', $account);
            $accountLocator = strtolower(end($accountParts));
        }

        // If hostname not set, construct from account locator
        if (empty($hostname) && !empty($accountLocator)) {
            $hostname = "{$accountLocator}.snowflakecomputing.com";
        }

        return [
            'account' => $account,
            'account_locator' => $accountLocator,
            'hostname' => $hostname,
            'database' => env('SNOWFLAKE_DATABASE', ''),
            'warehouse' => env('SNOWFLAKE_WAREHOUSE', ''),
            'schema' => env('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role' => env('SNOWFLAKE_ROLE', ''),
            'oauth_client_id' => env('SNOWFLAKE_OAUTH_CLIENT_ID', ''),
            'oauth_client_secret_raw' => env('SNOWFLAKE_OAUTH_CLIENT_SECRET', ''),
            'use_odbc' => true, // Always true for OAuth in native app
            'authenticator' => 'oauth'
        ];
    }

    /**
     * Check if native app has all required OAuth configuration
     *
     * @return bool
     */
    public static function hasValidOAuthConfig(): bool
    {
        $config = self::getNativeAppConfig();

        return !empty($config['account'])
            && !empty($config['oauth_client_id'])
            && !empty($config['oauth_client_secret_raw']);
    }

    /**
     * Merge native app config with user-provided config
     * User-provided values take precedence
     *
     * @param array $userConfig
     * @return array
     */
    public static function mergeWithUserConfig(array $userConfig): array
    {
        if (!self::isNativeApp()) {
            return $userConfig;
        }

        $nativeConfig = self::getNativeAppConfig();

        // Merge: user config overrides native config
        foreach ($nativeConfig as $key => $value) {
            if (!isset($userConfig[$key]) || $userConfig[$key] === '' || $userConfig[$key] === null) {
                $userConfig[$key] = $value;
            }
        }

        return $userConfig;
    }
}
