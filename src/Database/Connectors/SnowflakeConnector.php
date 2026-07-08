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
        $options = array_merge($this->getOptions($config), $this->options);
        $dsn = $this->getDsn($config);
        $connection = $this->createConnection($dsn, $config, $options);

        $this->applyQueryTag($connection, $config);

        return $connection;
    }

    /**
     * Tag the Snowflake session so query cost/usage can be attributed to this
     * DreamFactory service in ACCOUNT_USAGE.QUERY_HISTORY. Best-effort: a
     * tagging failure must never break an otherwise-healthy connection.
     *
     * @param \PDO  $pdo
     * @param array $config
     * @return void
     */
    protected function applyQueryTag($pdo, array $config)
    {
        try {
            $tag = array_filter([
                'app'       => 'dreamfactory',
                'service'   => $config['name'] ?? null,
                'database'  => $config['database'] ?? null,
                'schema'    => $config['schema'] ?? null,
                'warehouse' => $config['warehouse'] ?? null,
            ], fn ($v) => $v !== null && $v !== '');

            // Snowflake string literals quote single-quotes by doubling them.
            $json = str_replace("'", "''", json_encode($tag));
            $pdo->exec("ALTER SESSION SET QUERY_TAG = '{$json}'");
        } catch (\PDOException $e) {
            \Log::warning('Snowflake QUERY_TAG could not be set: ' . $e->getMessage());
        }
    }

    public function createConnection($dsn, array $config, array $options)
    {
        [$username, $password] = [
            $config['username'] ?? null, $config['password'] ?? null,
        ];

        // The config UI can leave password as "" (not null) when key-pair auth
        // is selected. Normalize an empty password to null so the branch below
        // chooses key-pair auth when a key is present, instead of falling
        // through to password auth with an empty password.
        if ($password === '') {
            $password = null;
        }

        try {
            if ($password === null && !empty($config['key'])) {
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

        if (!empty($hostname)) {
            $dsn .= "host={$hostname};";
        }

        if (!empty($account)) {
            $dsn .= "account={$account};";
        }

        if (!empty($database)) {
            $dsn .= "database={$database};";
        }

        // Schema is required in the Snowflake DSN. Default to PUBLIC (Snowflake's
        // own default schema) when the admin leaves it blank — matches the config
        // field hint ("Leave blank to work with the default schema") instead of
        // throwing on an otherwise-valid connection.
        $dsn .= "schema=" . (!empty($schema) ? $schema : 'PUBLIC') . ";";

        if (!empty($warehouse)) {
            $dsn .= "warehouse={$warehouse};";
        }

        if (!empty($role)) {
            $dsn .= "role={$role};";
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
