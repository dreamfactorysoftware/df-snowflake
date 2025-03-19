<?php

namespace DreamFactory\Core\Snowflake\Services;

use DreamFactory\Core\Snowflake\Resources\SnowflakeSchemaResource;
use DreamFactory\Core\Snowflake\Resources\SnowflakeTable as Table;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Resources\BaseRestResource;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
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
                    $this->getHeaderPram('schema')
                ];
                $paths[$pkey]['get']['parameters'] = array_merge($paths[$pkey]['get']['parameters'], $newParams);
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

        return $handlers;
    }

    private function getHeaderPram($name): array
    {
        return [
            "name" => $name,
            "description" => ucfirst($name) . " for database connection.",
            "schema" => [
                "type" => "string"
            ],
            "in" => "header",
            "required" => false
        ];
    }
}