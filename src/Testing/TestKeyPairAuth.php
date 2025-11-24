<?php

namespace DreamFactory\Core\Snowflake\Testing;

/**
 * A utility class to test Snowflake Key Pair Authentication
 * This is for development purposes only and should not be used in production.
 */
class TestKeyPairAuth
{
    /**
     * Test connection using key pair authentication
     *
     * @param string $account      Snowflake account
     * @param string $username     Snowflake username
     * @param string $keyFile      Path to private key file
     * @param string $passcode     Passphrase for private key (if any)
     * @param string $database     Database name
     * @param string $warehouse    Warehouse name
     * @param string $schema       Schema name
     * @return bool                True if connection successful, false otherwise
     */
    public static function testConnection(
        $account,
        $username,
        $keyFile,
        $passcode = '',
        $database = '',
        $warehouse = '',
        $schema = 'PUBLIC'
    ) {
        try {
            // Build DSN string
            $dsn = "snowflake:account={$account};";
            
            if (!empty($database)) {
                $dsn .= "database={$database};";
            }
            
            if (!empty($schema)) {
                $dsn .= "schema={$schema};";
            }
            
            if (!empty($warehouse)) {
                $dsn .= "warehouse={$warehouse};";
            }
            
            // Set up key pair authentication
            $dsn .= "authenticator=SNOWFLAKE_JWT;";
            
            // Check if key file exists
            if (!file_exists($keyFile)) {
                throw new \Exception("Private key file not found: {$keyFile}");
            }
            
            $dsn .= "priv_key_file={$keyFile};";
            
            if (!empty($passcode)) {
                $dsn .= "priv_key_file_pwd={$passcode};";
            }
            
            $dsn .= "application=DreamFactory_Test;";
            
            // Log connection attempt
            \Log::info("Testing Snowflake connection with DSN: " . preg_replace('/priv_key_file_pwd=[^;]+/', 'priv_key_file_pwd=******', $dsn));
            
            // Attempt connection
            $pdo = new \PDO($dsn, $username, "");
            
            // Execute a simple query to verify connection
            $stmt = $pdo->query("SELECT CURRENT_TIMESTAMP()");
            $result = $stmt->fetchColumn();
            
            \Log::info("Snowflake connection successful: " . $result);
            
            return true;
        } catch (\PDOException $e) {
            \Log::error("Snowflake connection error: " . $e->getMessage());
            return false;
        } catch (\Exception $e) {
            \Log::error("Test failed: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Generate a key pair for Snowflake authentication
     *
     * @param string $outputDir    Directory to save key files
     * @param string $passphrase   Optional passphrase for private key
     * @return array               Paths to created key files
     */
    public static function generateKeyPair($outputDir = null, $passphrase = null)
    {
        if (empty($outputDir)) {
            $outputDir = storage_path('app/keys/snowflake');
        }
        
        // Create directory if it doesn't exist
        if (!file_exists($outputDir)) {
            if (!mkdir($outputDir, 0700, true) && !is_dir($outputDir)) {
                throw new \Exception("Failed to create output directory: {$outputDir}");
            }
        }
        
        $privateKeyFile = $outputDir . '/rsa_key.p8';
        $publicKeyFile = $outputDir . '/rsa_key.pub';
        
        // Generate private key
        $config = [
            'digest_alg' => 'sha256',
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ];
        
        // Create private key
        $res = openssl_pkey_new($config);
        if ($res === false) {
            throw new \Exception("Failed to generate private key: " . openssl_error_string());
        }
        
        // Get private key
        if (empty($passphrase)) {
            openssl_pkey_export($res, $privateKey);
        } else {
            openssl_pkey_export($res, $privateKey, $passphrase);
        }
        
        // Save private key to file
        if (file_put_contents($privateKeyFile, $privateKey) === false) {
            throw new \Exception("Failed to save private key to file: {$privateKeyFile}");
        }
        chmod($privateKeyFile, 0600);
        
        // Get public key
        $publicKey = openssl_pkey_get_details($res)['key'];
        
        // Save public key to file
        if (file_put_contents($publicKeyFile, $publicKey) === false) {
            throw new \Exception("Failed to save public key to file: {$publicKeyFile}");
        }
        chmod($publicKeyFile, 0644);
        
        // Extract public key for SQL statement
        $sqlPublicKey = trim(preg_replace('/\s+/', ' ', str_replace(
            ['-----BEGIN PUBLIC KEY-----', '-----END PUBLIC KEY-----'],
            '',
            $publicKey
        )));
        
        return [
            'private_key_file' => $privateKeyFile,
            'public_key_file' => $publicKeyFile,
            'sql_public_key' => $sqlPublicKey,
            'sql_statement' => "ALTER USER username SET RSA_PUBLIC_KEY='{$sqlPublicKey}';"
        ];
    }
} 