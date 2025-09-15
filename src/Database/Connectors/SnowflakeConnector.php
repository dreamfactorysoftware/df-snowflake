<?php

namespace DreamFactory\Core\Snowflake\Database\Connectors;

use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;
use Exception;

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
        $options = array_merge($this->getOptions($config), $this->options);
        $dsn = $this->getDsn($config);
        $connection = $this->createConnection($dsn, $config, $options);

        return $connection;
    }

    public function createConnection($dsn, array $config, array $options)
    {
        [$username, $password] = [
            $config['username'] ?? null, $config['password'] ?? null,
        ];

        try {
            $authMethod = $config['authentication_method'] ?? 'password';
            
            switch ($authMethod) {
                case 'oauth':
                    return $this->createConnectionWithOAuth(
                        $dsn, $username, $config, $options
                    );
                case 'key_pair':
                    return $this->createConnectionWithKeyPairAuth(
                        $dsn, $username, $config, $options
                    );
                default:
                    // Legacy behavior: if no password and key exists, use key pair auth
                    if ($password === null && $config['key'] !== null) {
                        return $this->createConnectionWithKeyPairAuth(
                            $dsn, $username, $config, $options
                        );
                    }
                    return $this->createPdoConnection(
                        $dsn, $username, $password, $options
                    );
            }
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

    protected function createConnectionWithOAuth($dsn, $username, $config, $options)
    {
        try {
            $tokenPath = $config['oauth_token_path'] ?? '/snowflake/session/token';
            
            // DIAGNOSTIC LOGGING
            \Log::info("OAuth Debug - Token path: {$tokenPath}");
            \Log::info("OAuth Debug - File exists: " . (file_exists($tokenPath) ? 'YES' : 'NO'));
            
            // Check alternative token paths
            $possiblePaths = [
                '/snowflake/session/token',
                '/snowflake/session/oauth',
                '/snowflake/oauth/token'
            ];
            
            foreach ($possiblePaths as $path) {
                \Log::info("Checking OAuth path: {$path} - " . (file_exists($path) ? 'EXISTS' : 'NOT_FOUND'));
                if (file_exists($path) && $path !== $tokenPath) {
                    \Log::info("Alternative token path found: {$path}");
                }
            }
            
            if (!file_exists($tokenPath)) {
                // Check environment variables as fallback
                $envToken = $_ENV['SNOWFLAKE_OAUTH_TOKEN'] ?? getenv('SNOWFLAKE_OAUTH_TOKEN');
                if ($envToken) {
                    \Log::info("OAuth Debug - Using token from environment variable");
                    $token = $envToken;
                } else {
                    \Log::error("OAuth Debug - No token found in file or environment");
                    throw new \InvalidArgumentException(
                        "OAuth token file not found at: {$tokenPath}. " .
                        "This authentication method is only available in Snowflake Native App environments."
                    );
                }
            } else {
                if (!is_readable($tokenPath)) {
                    throw new \InvalidArgumentException(
                        "OAuth token file is not readable at: {$tokenPath}. Check file permissions."
                    );
                }

                $token = trim(file_get_contents($tokenPath));
                \Log::info("OAuth Debug - Token length: " . strlen($token));
                \Log::info("OAuth Debug - Token preview: " . substr($token, 0, 20) . '...');
            }
            
            if (empty($token)) {
                throw new \InvalidArgumentException(
                    "OAuth token file is empty at: {$tokenPath}. Token may have expired or not been generated yet."
                );
            }

            \Log::info("OAuth Debug - DSN: " . $dsn);

            // Pass token as PDO option instead of in DSN (like Python approach)
            $authOptions = array_merge($options, [
                'token' => $token,
                'authenticator' => 'oauth'
            ]);

            \Log::info("OAuth Debug - Attempting PDO connection with token as option");
            $pdo = new PDO($dsn, $username ?: '', '', $authOptions);
            
            // Apply any remaining PDO options
            foreach ($options as $key => $value) {
                $this->setConnectionAttribute($pdo, $key, $value);
            }
            
            \Log::info("OAuth Debug - PDO connection successful");
            return $pdo;
            
        } catch (\PDOException $e) {
            \Log::error('Snowflake OAuth authentication error: ' . $e->getMessage());
            \Log::error('OAuth Debug - PDO connection failed with token as option');
            throw new \InvalidArgumentException(
                'Failed to authenticate with OAuth token. ' .
                'Ensure you are running in a Snowflake Native App environment and the token is valid. ' .
                'Error: ' . $e->getMessage()
            );
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

        if (!empty($hostname)) {
            $dsn .= "host={$hostname};";
        }

        if (!empty($account)) {
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

        // Set up authentication based on method
        $authMethod = $config['authentication_method'] ?? 'password';
        
        if ($authMethod === 'oauth') {
            // OAuth authentication for Native Apps
            $dsn .= "authenticator=oauth;";
            // Note: Token is now passed as PDO option, not in DSN
        } elseif (($authMethod === 'key_pair') || (!empty($key) && $authMethod !== 'password')) {
            // Set up key pair authentication if a key is provided
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
}
