<?php
/**
 * Simple ODBC Connection Test Script
 *
 * This script tests the basic ODBC functionality without requiring
 * a full Snowflake account. It verifies that:
 * 1. PHP ODBC extension is loaded
 * 2. Snowflake ODBC driver is configured
 * 3. Our connector classes are loadable
 */

echo "=== Snowflake ODBC Connection Test ===\n\n";

// Test 1: Check if ODBC extension is loaded
echo "Test 1: Checking PHP ODBC extension...\n";
if (extension_loaded('odbc')) {
    echo "✓ ODBC extension is loaded\n\n";
} else {
    echo "✗ ODBC extension is NOT loaded\n";
    echo "Please install php-odbc extension\n";
    exit(1);
}

// Test 2: List available ODBC drivers
echo "Test 2: Listing available ODBC drivers...\n";
exec('odbcinst -q -d', $output, $return);
if ($return === 0) {
    echo "Available drivers:\n";
    foreach ($output as $line) {
        echo "  - $line\n";
    }

    if (in_array('[Snowflake]', $output)) {
        echo "✓ Snowflake ODBC driver is configured\n\n";
    } else {
        echo "✗ Snowflake ODBC driver NOT found\n\n";
    }
} else {
    echo "✗ Could not list ODBC drivers\n\n";
}

// Test 3: Try to load our connector classes
echo "Test 3: Loading Snowflake ODBC connector classes...\n";
// Use DreamFactory's autoloader
if (file_exists('/opt/dreamfactory/vendor/autoload.php')) {
    require_once '/opt/dreamfactory/vendor/autoload.php';
} elseif (file_exists(__DIR__ . '/../../vendor/autoload.php')) {
    require_once __DIR__ . '/../../vendor/autoload.php';
}

try {
    // Check if classes exist
    if (class_exists('DreamFactory\\Core\\Snowflake\\Database\\Connectors\\SnowflakeOdbcConnector')) {
        echo "✓ SnowflakeOdbcConnector class loaded\n";
    } else {
        echo "✗ SnowflakeOdbcConnector class NOT found\n";
    }

    if (class_exists('DreamFactory\\Core\\Snowflake\\Database\\SnowflakeOdbcConnection')) {
        echo "✓ SnowflakeOdbcConnection class loaded\n";
    } else {
        echo "✗ SnowflakeOdbcConnection class NOT found\n";
    }
} catch (Exception $e) {
    echo "✗ Error loading classes: " . $e->getMessage() . "\n";
}

echo "\n=== Test Connection (requires credentials) ===\n";
echo "To test with your Snowflake account, provide connection details:\n";
echo "\nExample usage:\n";
echo "php test_odbc.php --account=myaccount --username=myuser --password=mypass --database=mydb --schema=myschema --warehouse=mywh\n";
echo "\nFor OAuth testing:\n";
echo "php test_odbc.php --account=myaccount --token=<access_token> --database=mydb --schema=myschema --warehouse=mywh\n";

// Parse command line arguments
$options = getopt('', [
    'account:',
    'username::',
    'password::',
    'token::',
    'database:',
    'schema:',
    'warehouse:',
    'hostname::',
    'role::'
]);

if (!empty($options) && isset($options['account'], $options['database'], $options['schema'], $options['warehouse'])) {
    echo "\n=== Attempting Real Connection ===\n";

    try {
        $config = [
            'account' => $options['account'],
            'database' => $options['database'],
            'schema' => $options['schema'],
            'warehouse' => $options['warehouse'],
        ];

        if (!empty($options['hostname'])) {
            $config['hostname'] = $options['hostname'];
        }

        if (!empty($options['role'])) {
            $config['role'] = $options['role'];
        }

        // Determine authentication method
        if (!empty($options['token'])) {
            // OAuth
            $config['authenticator'] = 'oauth';
            $config['oauth_access_token'] = $options['token'];
            $config['username'] = $options['username'] ?? '';
            echo "Using OAuth authentication\n";
        } else {
            // Username/password
            $config['authenticator'] = 'snowflake';
            $config['username'] = $options['username'];
            $config['password'] = $options['password'];
            echo "Using username/password authentication\n";
        }

        $connector = new \DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeOdbcConnector();
        $connection = $connector->connect($config);

        if ($connection) {
            echo "✓ Successfully connected to Snowflake via ODBC!\n";

            // Try a simple query
            $result = odbc_exec($connection, "SELECT CURRENT_VERSION()");
            if ($result) {
                $row = odbc_fetch_array($result);
                echo "✓ Snowflake version: " . print_r($row, true) . "\n";
                odbc_free_result($result);
            }

            odbc_close($connection);
        } else {
            echo "✗ Failed to connect: " . odbc_errormsg() . "\n";
        }
    } catch (Exception $e) {
        echo "✗ Connection error: " . $e->getMessage() . "\n";
    }
}

echo "\n=== Test Complete ===\n";
