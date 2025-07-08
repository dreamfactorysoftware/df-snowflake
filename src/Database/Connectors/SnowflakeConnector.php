<?php

namespace DreamFactory\Core\Snowflake\Database\Connectors;

use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

class SnowflakeConnector extends Connector implements ConnectorInterface
{
    /**
     * The PDO connection options.
     *
     * @var array
     */
    protected $options = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ];

    public function connect(array $config)
    {
        // Log private link detection for debugging
        if (!empty($config['hostname'])) {
            $isPrivateLink = $this->isPrivateLinkHostname($config['hostname']);
            \Log::info('Snowflake connection attempt', [
                'hostname' => $config['hostname'],
                'is_private_link_detected' => $isPrivateLink,
                'private_link_enabled' => $config['private_link_enabled'] ?? false,
                'has_region' => !empty($config['region']),
                'account' => $config['account'] ?? 'not_specified'
            ]);
        }

        $options = array_merge($this->getOptions($config), $this->options);
        $dsn = $this->getDsn($config);
        
        // Log the DSN structure for debugging (without sensitive data)
        $dsnParts = explode(';', $dsn);
        $loggedDsn = [];
        foreach ($dsnParts as $part) {
            if (strpos($part, 'priv_key') === false && strpos($part, 'pwd') === false) {
                $loggedDsn[] = $part;
            }
        }
        \Log::debug('Snowflake DSN structure', ['dsn_parts' => $loggedDsn]);
        
        $connection = $this->createConnection($dsn, $config, $options);

        return $connection;
    }

    public function createConnection($dsn, array $config, array $options)
    {
        [$username, $password] = [
            $config['username'] ?? null, $config['password'] ?? null,
        ];

        try {
            if ($password === null && $config['key'] !== null) {
                return $this->createConnectionWithKeyPairAuth(
                    $dsn, $username, $config, $options
                );
            }
            return $this->createPdoConnection(
                $dsn, $username, $password, $options
            );
        } catch (Exception $e) {
            return $this->tryAgainIfCausedByLostConnection(
                $e, $dsn, $username, $password, $options
            );
        }
    }

    protected function createConnectionWithKeyPairAuth($dsn, $username, $config, $options)
    {
        // When using key pair authentication, we still pass the username
        // but leave password empty as the authentication is handled via the DSN
        // parameters for JWT authentication
        
        try {
            $pdo = new PDO($dsn, $username, "");
            
            // Apply any PDO options
            foreach ($options as $key => $value) {
                $this->setConnectionAttribute($pdo, $key, $value);
            }
            
            // Additional settings specific to key pair auth could be added here
            
            return $pdo;
        } catch (\PDOException $e) {
            // Log detailed error for easier debugging of key pair auth issues
            \Log::error('Snowflake key pair authentication error: ' . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Create a new PDO connection instance.
     *
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     * @return \PDO
     */
    protected function createPdoConnection($dsn, $username, $password, $options)
    {
        $pdo = new PDO($dsn, $username, $password);
        foreach ($options as $key => $value) {
            $this->setConnectionAttribute($pdo, $key, $value);
        }

        return $pdo;
    }
    
    /**
     * Set a PDO attribute on the connection.
     *
     * @param \PDO $pdo
     * @param mixed $key
     * @param mixed $value
     * @return void
     */
    protected function setConnectionAttribute($pdo, $key, $value)
    {
        try {
            if (is_int($key)) {
                $pdo->setAttribute($key, $value);
            } elseif (is_numeric($key)) {
                // If it's a numeric string, convert it to an integer
                $pdo->setAttribute((int) $key, $value);
            } elseif (is_string($key) && defined($key)) {
                // If it's a constant like 'PDO::ATTR_CASE'
                $pdo->setAttribute(constant($key), $value);
            } elseif (is_string($key) && strpos($key, 'PDO::') === 0) {
                // Handle strings like 'PDO::ATTR_DEFAULT_FETCH_MODE' that may not be defined constants
                // but are valid PDO attribute strings in some environments
                $constName = substr($key, 5); // Remove 'PDO::' prefix
                if (defined('PDO::' . $constName)) {
                    $pdo->setAttribute(constant('PDO::' . $constName), $value);
                } else {
                    // Attempt to set the attribute directly, let PDO handle validation
                    $pdo->setAttribute($key, $value);
                }
            } else {
                // For other string keys, attempt to set directly but catch any errors
                $pdo->setAttribute($key, $value);
            }
        } catch (\PDOException $e) {
            // Log warning about invalid attribute, but don't halt execution
            \Log::warning("Invalid PDO attribute: {$key}. Error: " . $e->getMessage());
        }
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        $dsn = "snowflake:";

        // Handle private link connections
        if (!empty($hostname)) {
            $isPrivateLink = $this->isPrivateLinkHostname($hostname);
            
            // Always use the provided hostname
            $dsn .= "host={$hostname};";
            
            // For private links, account is usually embedded in hostname
            // Only add account parameter if explicitly different or if it's not a private link
            if (!$isPrivateLink && !empty($account)) {
                // Standard hostname with separate account
                $dsn .= "account={$account};";
            } elseif ($isPrivateLink && !empty($account)) {
                // For private links, extract account from hostname and compare
                $extractedAccount = $this->extractAccountFromPrivateLinkHostname($hostname);
                
                // Only add account if it's different from what's in hostname
                // This handles edge cases where account parameter provides additional context
                if ($extractedAccount !== $account) {
                    \Log::warning('Snowflake private link hostname account mismatch', [
                        'hostname' => $hostname,
                        'hostname_account' => $extractedAccount,
                        'config_account' => $account,
                        'using_hostname_account' => true
                    ]);
                    // Use hostname account for consistency, but log the discrepancy
                }
                // Note: We don't add account parameter for private links as it's redundant
            }
        } elseif (!empty($account)) {
            // Traditional account-based connection without explicit hostname
            $dsn .= "account={$account};";
        }

        if (!empty($database)) {
            $dsn .= "database={$database};";
        }

        if (!empty($schema)) {
            $dsn .= "schema={$schema};";
        } else {
            throw new \InvalidArgumentException("Schema not given, required.");
        }

        if (!empty($warehouse)) {
            $dsn .= "warehouse={$warehouse};";
        }

        if (!empty($role)) {
            $dsn .= "role={$role};";
        }

        // Handle private link specific configurations
        if (!empty($private_link_enabled) && $private_link_enabled) {
            // Add any private link specific DSN parameters if needed
            // Snowflake might require specific parameters for private link connections
            if (!empty($region)) {
                $dsn .= "region={$region};";
            }
        }

        // Set up key pair authentication if a key is provided
        if (!empty($key)) {
            // Use JWT authentication with Snowflake
            $dsn .= "authenticator=SNOWFLAKE_JWT;";
            
            // Get absolute path to the key file - important for reliable connections
            $keyPath = realpath($key);
            if (!$keyPath || !file_exists($keyPath)) {
                throw new \InvalidArgumentException("Private key file not found at: {$key}");
            }
            
            // Escape special characters in DSN values to prevent injection
            $escapedKeyPath = $this->escapeDsnValue($keyPath);
            $dsn .= "priv_key_file={$escapedKeyPath};";
            
            // Add passcode for the private key if provided
            if (!empty($passcode)) {
                $escapedPasscode = $this->escapeDsnValue($passcode);
                $dsn .= "priv_key_file_pwd={$escapedPasscode};";
            }
        }

        $dsn .= "application=DreamFactory_DreamFactory;";

        return $dsn;
    }

    /**
     * Extract account name from a private link hostname.
     *
     * @param string $hostname
     * @return string|null
     */
    protected function extractAccountFromPrivateLinkHostname($hostname)
    {
        // For private link hostnames like: myaccount.privatelink.snowflakecomputing.com
        if (preg_match('/^([^.]+)\.privatelink\.snowflakecomputing\.com$/', $hostname, $matches)) {
            return $matches[1];
        }
        
        // For other private link patterns, try to extract account from first segment
        if (preg_match('/^([^.]+)\./', $hostname, $matches)) {
            return $matches[1];
        }
        
        return null;
    }

    /**
     * Determine if a hostname is a Snowflake private link endpoint.
     *
     * @param string $hostname
     * @return bool
     */
    protected function isPrivateLinkHostname($hostname)
    {
        // Check for common private link patterns
        $privateLinkPatterns = [
            '/\.privatelink\.snowflakecomputing\.com$/',
            '/\.internal\.snowflake\./', // Custom private endpoints
            '/vpce-[a-f0-9\-]+\.snowflake\./', // AWS VPC Endpoint patterns
        ];

        foreach ($privateLinkPatterns as $pattern) {
            if (preg_match($pattern, $hostname)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Escape special characters in DSN values to prevent DSN injection.
     *
     * @param string $value The value to escape
     * @return string The escaped value
     */
    protected function escapeDsnValue($value)
    {
        // Escape characters that could be used for DSN injection
        // Primarily semicolons and equals signs which have special meaning in DSN strings
        return str_replace([';', '='], ['\\;', '\\='], $value);
    }

    /**
     * Debug method to validate DSN generation without connecting.
     * Can be called via API for testing purposes.
     *
     * @param array $config
     * @return array
     */
    public function debugConnection(array $config)
    {
        try {
            $dsn = $this->getDsn($config);
            $isPrivateLink = $this->isPrivateLinkHostname($config['hostname'] ?? '');
            $extractedAccount = null;
            $accountHandling = 'not_applicable';
            
            if ($isPrivateLink && !empty($config['hostname'])) {
                $extractedAccount = $this->extractAccountFromPrivateLinkHostname($config['hostname']);
                $providedAccount = $config['account'] ?? null;
                
                if (!empty($providedAccount)) {
                    if ($extractedAccount === $providedAccount) {
                        $accountHandling = 'redundant_but_matching';
                    } elseif ($extractedAccount !== $providedAccount) {
                        $accountHandling = 'conflicting';
                    }
                } else {
                    $accountHandling = 'hostname_only_preferred';
                }
            }
            
            return [
                'success' => true,
                'dsn' => $dsn,
                'is_private_link' => $isPrivateLink,
                'detected_patterns' => $this->getDetectedPatterns($config['hostname'] ?? ''),
                'account_handling' => [
                    'status' => $accountHandling,
                    'extracted_from_hostname' => $extractedAccount,
                    'provided_in_config' => $config['account'] ?? null,
                    'included_in_dsn' => !$isPrivateLink && !empty($config['account'])
                ],
                'config_summary' => [
                    'hostname' => $config['hostname'] ?? null,
                    'account' => $config['account'] ?? null,
                    'private_link_enabled' => $config['private_link_enabled'] ?? false,
                    'region' => $config['region'] ?? null,
                ],
                'validation' => $this->validatePrivateLinkConfig($config)
            ];
        } catch (\Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'config_provided' => array_keys($config)
            ];
        }
    }

    /**
     * Get which patterns matched for hostname detection
     */
    protected function getDetectedPatterns($hostname)
    {
        if (empty($hostname)) return [];
        
        $patterns = [
            'privatelink.snowflakecomputing.com' => '/\.privatelink\.snowflakecomputing\.com$/',
            'internal.snowflake' => '/\.internal\.snowflake\./',
            'vpce-aws' => '/vpce-[a-f0-9\-]+\.snowflake\./',
        ];
        
        $matched = [];
        foreach ($patterns as $name => $pattern) {
            if (preg_match($pattern, $hostname)) {
                $matched[] = $name;
            }
        }
        
        return $matched;
    }

    /**
     * Validate private link specific configuration
     */
    protected function validatePrivateLinkConfig($config)
    {
        $warnings = [];
        $recommendations = [];
        
        $hostname = $config['hostname'] ?? '';
        $account = $config['account'] ?? '';
        $isPrivateLink = $this->isPrivateLinkHostname($hostname);
        $privateLinkEnabled = $config['private_link_enabled'] ?? false;
        
        // Check alignment between hostname and flag
        if ($isPrivateLink && !$privateLinkEnabled) {
            $warnings[] = 'Private link hostname detected but private_link_enabled is false';
            $recommendations[] = 'Set private_link_enabled=true for optimal private link support';
        }
        
        if (!$isPrivateLink && $privateLinkEnabled) {
            $warnings[] = 'private_link_enabled is true but hostname does not appear to be a private link endpoint';
        }
        
        // Check for account parameter with private links
        if ($isPrivateLink && !empty($account)) {
            $extractedAccount = $this->extractAccountFromPrivateLinkHostname($hostname);
            if ($extractedAccount && $extractedAccount !== $account) {
                $warnings[] = "Account mismatch: hostname suggests '{$extractedAccount}' but account parameter is '{$account}'";
                $recommendations[] = 'For private links, account is embedded in hostname. Consider removing account parameter or ensuring it matches hostname.';
            } elseif ($extractedAccount && $extractedAccount === $account) {
                $recommendations[] = 'Account parameter is redundant for private links as account is embedded in hostname. You can safely remove the account parameter.';
            }
        }
        
        // Check for region specification with private links
        if ($isPrivateLink && empty($config['region'])) {
            $recommendations[] = 'Consider specifying region parameter for private link connections';
        }
        
        return [
            'is_valid' => empty($warnings),
            'warnings' => $warnings,
            'recommendations' => $recommendations
        ];
    }
}
