<?php

namespace DreamFactory\Core\Snowflake\Services;

use DreamFactory\Core\Snowflake\Resources\SnowflakeSchemaResource;
use DreamFactory\Core\Snowflake\Resources\SnowflakeStoredFunction;
use DreamFactory\Core\Snowflake\Resources\SnowflakeTable as Table;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use Arr;

/**
 * Class SnowflakeDb
 *
 * @package DreamFactory\Core\Snowflake\Services
 */
class SnowflakeDb extends SqlDb
{
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $prefix = parent::getConfigBasedCachePrefix();
        $this->setConfigBasedCachePrefix($prefix);
    }

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'snowflake';
        
        // Auto-detect OAuth environment and set default configuration
        static::detectAndConfigureOAuthEnvironment($config);
        
        // Handle key file upload if provided
        if (!empty($config['key']) && is_array($config['key']) && !empty($config['key']['tmp_name'])) {
            $keyFile = $config['key'];
            $config['key'] = static::handleKeyFileUpload($keyFile);
        }
        
        parent::adaptConfig($config);
    }

    /**
     * Handle the upload and storage of a private key file for key pair authentication
     *
     * @param array $keyFile The uploaded file information
     * @return string The path to the stored key file
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected static function handleKeyFileUpload(array $keyFile)
    {
        // Create storage directory if it doesn't exist
        $storageDir = storage_path('app/keys/snowflake');
        if (!file_exists($storageDir)) {
            if (!mkdir($storageDir, 0700, true) && !is_dir($storageDir)) {
                throw new \DreamFactory\Core\Exceptions\BadRequestException(
                    'Failed to create key storage directory: ' . $storageDir
                );
            }
        }
        
        // Handle both direct file uploads and UI-processed uploads
        $tmpName = isset($keyFile['tmp_name']) ? $keyFile['tmp_name'] : null;
        $fileName = isset($keyFile['name']) ? $keyFile['name'] : null;
        
        // Special handling for UI uploads that may have a different structure
        if (!$tmpName && isset($keyFile['_file'])) {
            // Create a strictly sanitized version of the keyFile for logging
            // Only include safe, non-sensitive fields
            $safeToLog = [
                'name' => isset($keyFile['name']) ? $keyFile['name'] : 'unknown',
                'type' => isset($keyFile['type']) ? $keyFile['type'] : 'unknown',
                'size' => isset($keyFile['size']) ? $keyFile['size'] : 'unknown',
                '_file' => '[FILE_REFERENCE]',
                'has_content' => isset($keyFile['content']) ? 'yes' : 'no'
            ];
            
            // Log only the safe information about the file structure
            \Log::info('Handling UI file upload: ' . json_encode($safeToLog));
            
            // For UI uploads, extract the file from the request and save it
            $base64Content = isset($keyFile['content']) ? $keyFile['content'] : null;
            
            if ($base64Content) {
                $fileContent = base64_decode($base64Content);
                $tmpName = tempnam(sys_get_temp_dir(), 'snowflake_key_');
                file_put_contents($tmpName, $fileContent);
            }
        }
        
        // Validate the file exists
        if (!$tmpName || !file_exists($tmpName)) {
            throw new \DreamFactory\Core\Exceptions\BadRequestException(
                'No valid key file was uploaded. Please try again.'
            );
        }
        
        // Validate file format - check if it's a valid PEM file
        $fileContent = file_get_contents($tmpName);
        if (!self::validateKeyFormat($fileContent)) {
            throw new \DreamFactory\Core\Exceptions\BadRequestException(
                'Invalid key file format. Please upload a valid PEM-formatted private key file.'
            );
        }
        
        // Generate a filename that preserves the original name when possible
        if (!empty($fileName)) {
            // Get file extension from original filename
            $extension = pathinfo($fileName, PATHINFO_EXTENSION);
            if (empty($extension)) {
                $extension = 'pem'; // Default extension if none provided
            }
            
            // Use original name but add uniqueness to prevent collisions
            $baseName = pathinfo($fileName, PATHINFO_FILENAME);
            $uniqueSuffix = '_' . substr(md5(uniqid('', true)), 0, 8);
            $uniqueFileName = $baseName . $uniqueSuffix . '.' . $extension;
        } else {
            // No original filename, use a completely generated one
            $uniqueFileName = 'snowflake_' . md5(uniqid('', true)) . '.pem';
        }
        $keyPath = $storageDir . '/' . $uniqueFileName;
        
        // Move the uploaded file to the storage location
        if (file_exists($tmpName)) {
            if (is_uploaded_file($tmpName)) {
                if (!move_uploaded_file($tmpName, $keyPath)) {
                    throw new \DreamFactory\Core\Exceptions\BadRequestException(
                        'Failed to store key file. Please check permissions and try again.'
                    );
                }
            } else {
                // For files that weren't uploaded via web form (API or UI)
                if (!copy($tmpName, $keyPath)) {
                    throw new \DreamFactory\Core\Exceptions\BadRequestException(
                        'Failed to store key file. Please check permissions and try again.'
                    );
                }
                // Clean up temp file
                @unlink($tmpName);
            }
            
            // Make sure the file has the right permissions (readable only by owner)
            chmod($keyPath, 0600);
            
            \Log::info('Snowflake key file uploaded successfully to: ' . $keyPath);
            return $keyPath;
        }
        
        // If we reach here, we couldn't handle the file upload correctly
        // Create a strictly sanitized version with only non-sensitive info
        $safeToLog = [
            'name' => isset($keyFile['name']) ? $keyFile['name'] : 'unknown',
            'type' => isset($keyFile['type']) ? $keyFile['type'] : 'unknown',
            'size' => isset($keyFile['size']) ? $keyFile['size'] : 'unknown',
            'has_content' => isset($keyFile['content']) ? 'yes' : 'no'
        ];
        \Log::error('Failed to process Snowflake key file upload: ' . json_encode($safeToLog));
        throw new \DreamFactory\Core\Exceptions\BadRequestException(
            'Failed to process key file upload. Please check file format and try again.'
        );
    }
    
    /**
     * Validate that the uploaded file is a proper PEM-formatted private key
     * 
     * @param string $fileContent The content of the uploaded file
     * @return boolean True if the file appears to be a valid private key
     */
    protected static function validateKeyFormat($fileContent)
    {
        // Check for common PEM private key formats
        $privateKeyPatterns = [
            '/-----BEGIN PRIVATE KEY-----.*-----END PRIVATE KEY-----/s',
            '/-----BEGIN RSA PRIVATE KEY-----.*-----END RSA PRIVATE KEY-----/s',
            '/-----BEGIN ENCRYPTED PRIVATE KEY-----.*-----END ENCRYPTED PRIVATE KEY-----/s',
            '/-----BEGIN OPENSSH PRIVATE KEY-----.*-----END OPENSSH PRIVATE KEY-----/s',
            '/-----BEGIN DSA PRIVATE KEY-----.*-----END DSA PRIVATE KEY-----/s',
            '/-----BEGIN EC PRIVATE KEY-----.*-----END EC PRIVATE KEY-----/s',
        ];
        
        foreach ($privateKeyPatterns as $pattern) {
            if (preg_match($pattern, $fileContent)) {
                return true;
            }
        }
        
        // For PKCS#8 format (used by Snowflake), additional checks
        // Look for the specific key header and ASN.1 sequence structure
        $hasPkcs8Header = strpos($fileContent, '-----BEGIN PRIVATE KEY-----') !== false;
        $hasValidASN1 = (bool) preg_match('/\x30[\x80-\xff]*\x02\x01\x00\x30/', $fileContent);
        
        if ($hasPkcs8Header && $hasValidASN1) {
            return true;
        }
        
        // Special check for Snowflake's rsa_key.p8 format
        if (strpos($fileContent, 'SEQUENCE') !== false && 
            strpos($fileContent, 'OBJECT IDENTIFIER') !== false &&
            strpos($fileContent, 'rsaEncryption') !== false) {
            return true;
        }
        
        return false;
    }

    /**
     * Detect if running in a Snowflake Native App environment and configure OAuth settings
     *
     * @param array $config Configuration array to modify
     * @return void
     */
    protected static function detectAndConfigureOAuthEnvironment(array &$config)
    {
        \Log::info('=== OAuth Environment Detection Started ===');

        // Log all relevant environment variables for debugging
        $envVarsToCheck = [
            'SNOWFLAKE_HOST',
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_ROLE',
            'SNOWFLAKE_OAUTH_TOKEN'
        ];

        \Log::info('Environment Variables Check:');
        foreach ($envVarsToCheck as $envVar) {
            $envValue = $_ENV[$envVar] ?? getenv($envVar);
            if ($envValue !== false && $envValue !== null && $envValue !== '') {
                // Mask sensitive values
                if (strpos($envVar, 'TOKEN') !== false) {
                    \Log::info("  {$envVar}: ***MASKED*** (length: " . strlen($envValue) . ")");
                } else {
                    \Log::info("  {$envVar}: {$envValue}");
                }
            } else {
                \Log::info("  {$envVar}: NOT_SET");
            }
        }

        // Check if we're in a Snowflake Native App environment
        $possibleTokenPaths = [
            '/snowflake/session/token',
            '/snowflake/session/oauth',
            '/snowflake/oauth/token'
        ];

        $defaultTokenPath = '/snowflake/session/token';
        $isNativeAppEnvironment = false;
        $foundTokenPath = null;

        \Log::info('Token File Detection:');
        foreach ($possibleTokenPaths as $path) {
            $exists = file_exists($path);
            $readable = $exists ? is_readable($path) : false;
            $size = ($exists && $readable) ? filesize($path) : 0;

            \Log::info("  {$path}: " .
                ($exists ? 'EXISTS' : 'NOT_FOUND') .
                ($readable ? ', READABLE' : ($exists ? ', NOT_READABLE' : '')) .
                ($size > 0 ? ", SIZE: {$size} bytes" : ($exists ? ', EMPTY' : '')));

            if ($exists && $readable && $size > 0) {
                $isNativeAppEnvironment = true;
                $foundTokenPath = $path;
                if ($path !== $defaultTokenPath) {
                    \Log::info("  Found token at alternative path: {$path}");
                    $config['oauth_token_path'] = $path; // Update config with correct path
                }

                // Read and log token info (but mask the actual token)
                $tokenContent = trim(file_get_contents($path));
                if (!empty($tokenContent)) {
                    \Log::info("  Token content: length=" . strlen($tokenContent) .
                              ", preview=" . substr($tokenContent, 0, 20) . "...");
                } else {
                    \Log::warning("  Token file exists but is empty: {$path}");
                }
                break;
            }
        }

        // Only apply environment variable auto-configuration if:
        // 1. OAuth authentication method is explicitly selected, OR
        // 2. No authentication method is set and we're in a native app environment
        $authMethod = $config['authentication_method'] ?? null;
        $shouldApplyOAuthConfig = ($authMethod === 'oauth') ||
                                 ($isNativeAppEnvironment && empty($authMethod));

        if ($shouldApplyOAuthConfig) {
            if ($authMethod === 'oauth') {
                \Log::info('OAuth authentication method selected - applying OAuth configuration');
            } else {
                \Log::info('Detected Snowflake Native App environment with no auth method set - applying OAuth configuration');
            }

            if ($foundTokenPath) {
                \Log::info('OAuth token available at: ' . $foundTokenPath);
            }

            \Log::info('Configuration priority: Config values > Environment variables');

            // Only use environment variables as fallback when config values are not provided
            // Config values take priority over environment variables
            if (empty($config['account'])) {
                $envAccount = $_ENV['SNOWFLAKE_ACCOUNT'] ?? getenv('SNOWFLAKE_ACCOUNT');
                if ($envAccount) {
                    $config['account'] = $envAccount;
                    \Log::info('  account: set from SNOWFLAKE_ACCOUNT environment variable');
                } else {
                    \Log::info('  account: not configured and SNOWFLAKE_ACCOUNT environment variable not found');
                }
            } else {
                \Log::info('  account: using configured value (ignoring environment variable)');
            }

            if (empty($config['hostname'])) {
                $envHost = $_ENV['SNOWFLAKE_HOST'] ?? getenv('SNOWFLAKE_HOST');
                if ($envHost) {
                    $config['hostname'] = $envHost;
                    \Log::info('  hostname: set from SNOWFLAKE_HOST environment variable');
                } else {
                    \Log::info('  hostname: not configured and SNOWFLAKE_HOST environment variable not found');
                }
            } else {
                \Log::info('  hostname: using configured value (ignoring environment variable)');
            }

            if (empty($config['database'])) {
                $envDatabase = $_ENV['SNOWFLAKE_DATABASE'] ?? getenv('SNOWFLAKE_DATABASE');
                if ($envDatabase) {
                    $config['database'] = $envDatabase;
                    \Log::info('  database: set from SNOWFLAKE_DATABASE environment variable');
                } else {
                    \Log::info('  database: not configured and SNOWFLAKE_DATABASE environment variable not found');
                }
            } else {
                \Log::info('  database: using configured value (ignoring environment variable)');
            }

            if (empty($config['warehouse'])) {
                $envWarehouse = $_ENV['SNOWFLAKE_WAREHOUSE'] ?? getenv('SNOWFLAKE_WAREHOUSE');
                if ($envWarehouse) {
                    $config['warehouse'] = $envWarehouse;
                    \Log::info('  warehouse: set from SNOWFLAKE_WAREHOUSE environment variable');
                } else {
                    \Log::info('  warehouse: not configured and SNOWFLAKE_WAREHOUSE environment variable not found');
                }
            } else {
                \Log::info('  warehouse: using configured value (ignoring environment variable)');
            }

            if (empty($config['schema'])) {
                $envSchema = $_ENV['SNOWFLAKE_SCHEMA'] ?? getenv('SNOWFLAKE_SCHEMA');
                if ($envSchema) {
                    $config['schema'] = $envSchema;
                    \Log::info('  schema: set from SNOWFLAKE_SCHEMA environment variable');
                } else {
                    \Log::info('  schema: not configured and SNOWFLAKE_SCHEMA environment variable not found');
                }
            } else {
                \Log::info('  schema: using configured value (ignoring environment variable)');
            }

            if (empty($config['role'])) {
                $envRole = $_ENV['SNOWFLAKE_ROLE'] ?? getenv('SNOWFLAKE_ROLE');
                if ($envRole) {
                    $config['role'] = $envRole;
                    \Log::info('  role: set from SNOWFLAKE_ROLE environment variable');
                } else {
                    \Log::info('  role: not configured and SNOWFLAKE_ROLE environment variable not found');
                }
            } else {
                \Log::info('  role: using configured value (ignoring environment variable)');
            }

            // Set OAuth token path if not already configured
            if (empty($config['oauth_token_path'])) {
                $config['oauth_token_path'] = $defaultTokenPath;
                \Log::info('  oauth_token_path: set to default path');
            } else {
                \Log::info('  oauth_token_path: using configured value');
            }
        } else {
            \Log::info('OAuth auto-configuration skipped - authentication method is not oauth');
        }

        // Log environment detection result
        if ($isNativeAppEnvironment) {
            \Log::info('RESULT: Snowflake Native App environment detected - OAuth authentication is available');
            \Log::info('Found token path: ' . $foundTokenPath);
        } else {
            \Log::info('RESULT: Standard Snowflake environment - using traditional authentication methods');
            \Log::info('No OAuth token files found in expected locations');
        }

        \Log::info('=== OAuth Environment Detection Completed ===');
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();
        $paths = (array)Arr::get($base, 'paths');
        foreach ($paths as $pkey => $path) {
            foreach ($path as $rkey => $resource) {
                if ($rkey === 'patch' || $rkey === 'put') {
                    unset($paths[$pkey][$rkey]);
                    continue;
                }
            }
        }
        foreach ($paths as $pkey => $path) {
            if ($pkey !== '/' && isset($path['get']) && isset($path['get']['parameters'])) {
                $newParams = [
                    $this->getHeaderPram('hostname'),
                    $this->getHeaderPram('account'),
                    $this->getHeaderPram('username'),
                    $this->getHeaderPram('password'),
                    $this->getHeaderPram('key'),
                    $this->getHeaderPram('passcode'),
                    $this->getHeaderPram('role'),
                    $this->getHeaderPram('database'),
                    $this->getHeaderPram('warehouse'),
                    $this->getHeaderPram('schema'),
                    $this->getHeaderPram('authentication_method', 'Authentication method (password, key_pair, oauth)'),
                    $this->getHeaderPram('oauth_token_path', 'Path to OAuth token file for Native App environments')
                ];
                $paths[$pkey]['get']['parameters'] = array_merge($paths[$pkey]['get']['parameters'], $newParams);
            }
            
            if (strpos($pkey, '/_func/') !== false) {
                if (isset($path['post']['parameters'])) {
                    $path['post']['parameters'][] = $this->getHeaderPram('X-Database-Name', 'Database name for cross-database function calls');
                    $path['post']['parameters'][] = $this->getHeaderPram('X-Schema-Name', 'Schema name for function calls');
                } else if (isset($path['post'])) {
                    $path['post']['parameters'] = [
                        $this->getHeaderPram('X-Database-Name', 'Database name for cross-database function calls'),
                        $this->getHeaderPram('X-Schema-Name', 'Schema name for function calls')
                    ];
                }
                $paths[$pkey] = $path;
            }
        }
        $base['paths'] = $paths;

        return $base;
    }

    public static function getDriverName()
    {
        return 'snowflake';
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[Table::RESOURCE_NAME] = [
            'name' => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label' => 'Table',
        ];

        $handlers[SnowflakeSchemaResource::RESOURCE_NAME] = [
            'name'       => SnowflakeSchemaResource::RESOURCE_NAME,
            'class_name' => SnowflakeSchemaResource::class,
            'label'      => 'Schema Table',
        ];

        $handlers[StoredProcedure::RESOURCE_NAME] = [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedure',
        ];

        $handlers[StoredFunction::RESOURCE_NAME] = [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => SnowflakeStoredFunction::class,
            'label'      => 'Stored Function',
        ];

        return $handlers;
    }

    private function getHeaderPram($name, $description = null): array
    {
        return [
            "name" => $name,
            "description" => $description ?: ucfirst($name) . " for database connection.",
            "schema" => [
                "type" => "string"
            ],
            "in" => "header",
            "required" => false
        ];
    }
}