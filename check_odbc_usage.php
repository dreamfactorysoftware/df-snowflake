<?php
require "/opt/dreamfactory/vendor/autoload.php";

$service = \DreamFactory\Core\Models\Service::where("name", "snowflake_test")->first();
if ($service) {
    $config = $service->config;
    echo "=== Snowflake Service Configuration ===\n";
    echo "use_odbc: " . ($config["use_odbc"] ?? "not set") . "\n";
    echo "authenticator: " . ($config["authenticator"] ?? "not set") . "\n";

    // Check what the ServiceProvider will do
    $useOdbc = !empty($config["use_odbc"]) || (!empty($config["authenticator"]) && $config["authenticator"] === "oauth");
    echo "\n=== ODBC Decision ===\n";
    echo "Will use ODBC: " . ($useOdbc ? "YES ✓" : "NO - will use PDO") . "\n";

    if ($useOdbc) {
        echo "\nReason: ";
        if (!empty($config["use_odbc"])) {
            echo "use_odbc flag is set\n";
        } elseif (!empty($config["authenticator"]) && $config["authenticator"] === "oauth") {
            echo "authenticator is set to 'oauth'\n";
        }
    }

    // Show which connector class will be used
    echo "\n=== Connector Classes ===\n";
    echo "Will instantiate: " . ($useOdbc ? "SnowflakeOdbcConnector" : "SnowflakeConnector (PDO)") . "\n";
    echo "Will create: " . ($useOdbc ? "SnowflakeOdbcConnection" : "SnowflakeConnection (PDO)") . "\n";
} else {
    echo "Service 'snowflake_test' not found\n";
}
