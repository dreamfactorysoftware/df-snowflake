<?php
namespace DreamFactory\Core\Snowflake;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeConnector;
use DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeOdbcConnector;
use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use DreamFactory\Core\Snowflake\Database\SnowflakeConnection;
use DreamFactory\Core\Snowflake\Database\SnowflakeOdbcConnection;
use DreamFactory\Core\Snowflake\Models\SnowflakeDbConfig;
use DreamFactory\Core\Snowflake\Services\SnowflakeDb;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Register OAuth routes early (before service routing)
        $this->registerRoutes();

        $this->app->resolving('df.db.schema', function (DbSchemaExtensions $db){
            $db->extend('snowflake', function ($connection){
                return new SnowflakeSchema($connection);
            });
        });

        $this->app->resolving('db', function (DatabaseManager $db){
            $db->extend('snowflake', function ($config){
                $this->checkUrlParams($config['config']);
                $this->checkHeaders($config['config']);

                // Determine whether to use ODBC or PDO based on config
                // Check both top-level and nested config locations
                $useOdbcTop = !empty($config['use_odbc']);
                $useOdbcNested = !empty($config['config']['use_odbc']);
                $authTop = ($config['authenticator'] ?? null) === 'oauth';
                $authNested = ($config['config']['authenticator'] ?? null) === 'oauth';
                // Always use ODBC when running inside SPCS (Snowpark Container Services)
                $inSpcs = !empty(getenv('SNOWFLAKE_HOST')) && !empty(getenv('SNOWFLAKE_ACCOUNT'));

                $useOdbc = $useOdbcTop || $useOdbcNested || $authTop || $authNested || $inSpcs;

                \Log::info('Snowflake connector selection', [
                    'use_odbc_top' => $config['use_odbc'] ?? 'not set',
                    'use_odbc_nested' => $config['config']['use_odbc'] ?? 'not set',
                    'authenticator_top' => $config['authenticator'] ?? 'not set',
                    'authenticator_nested' => $config['config']['authenticator'] ?? 'not set',
                    'in_spcs' => $inSpcs ? 'YES' : 'NO',
                    'will_use_odbc' => $useOdbc ? 'YES' : 'NO'
                ]);

                if ($useOdbc) {
                    // Use ODBC connector
                    \Log::info('Using SnowflakeOdbcConnector');
                    $connector = new SnowflakeOdbcConnector();
                    $odbcResource = $connector->connect($config);
                    $connection = new SnowflakeOdbcConnection($odbcResource, $config['database'], '', $config);

                    // Set up reconnector to handle Laravel connection lifecycle
                    $connection->setReconnector(function ($connection) use ($connector, $config) {
                        $odbcResource = $connector->connect($config);
                        $connection->setPdo($odbcResource);
                    });

                    return $connection;
                } else {
                    // Use PDO connector (legacy)
                    \Log::info('Using SnowflakeConnector (PDO)');
                    $connector = new SnowflakeConnector();
                    $connection = $connector->connect($config);
                    return new SnowflakeConnection($connection, $config['database'], '', $config);
                }
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'snowflake',
                    'label'           => 'Snowflake',
                    'description'     => 'Database service supporting Snowflake connections.',
                    'group'           => 'Database', // or if you want to use defined groups use DreamFactory\Core\Enums\ServiceTypeGroups, ServiceTypeGroups::REMOTE
                    'subscription_required' => LicenseLevel::GOLD, // don't specify this if you want the service be used on Open Source version
                    'config_handler'  => SnowflakeDbConfig::class,
                    'factory'         => function ($config) {
                        $this->checkUrlParams($config['config']);
                        $this->checkHeaders($config['config']);
                        return new SnowflakeDb($config);
                    },
                ])
            );
        });
    }

    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }

    /**
     * Register OAuth routes
     */
    protected function registerRoutes()
    {
        if (!$this->app->routesAreCached()) {
            // Use _oauth prefix to avoid collision with services named "snowflake"
            // Callback needs to be public (Snowflake redirects here)
            \Route::get('api/v2/_oauth/snowflake/callback', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeOAuthController@callback');

            // Other OAuth endpoints
            \Route::prefix('api/v2/_oauth/snowflake')
                ->group(function () {
                    \Route::get('authorize', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeOAuthController@authorize');
                    \Route::post('refresh', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeOAuthController@refresh');
                    \Route::get('status', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeOAuthController@status');
                });

            // Discovery endpoints — populate the admin-UI picker from the app's own SPCS identity.
            // No service_id required; these run before a DF service is created.
            \Route::prefix('api/v2/_snowflake/discover')
                ->group(function () {
                    \Route::get('databases', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeDiscoveryController@databases');
                    \Route::get('schemas', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeDiscoveryController@schemas');
                    \Route::get('warehouses', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeDiscoveryController@warehouses');
                    \Route::get('roles', 'DreamFactory\Core\Snowflake\Http\Controllers\SnowflakeDiscoveryController@roles');
                });
        }
    }


    protected function checkHeaders(&$config)
    {
        $this->substituteConfig('hostname', 'header', $config);
        $this->substituteConfig('account', 'header', $config);
        $this->substituteConfig('account_locator', 'header', $config);
        $this->substituteConfig('database', 'header', $config);
        $this->substituteConfig('schema', 'header', $config);
        $this->substituteConfig('warehouse', 'header', $config);
        $this->substituteConfig('username', 'header', $config);
        $this->substituteConfig('password', 'header', $config);
        $this->substituteConfig('key', 'header', $config);
        $this->substituteConfig('passcode', 'header', $config);
        $this->substituteConfig('role', 'header', $config);
    }

    protected function checkUrlParams(&$config)
    {
        $this->substituteConfig('hostname', 'url', $config);
        $this->substituteConfig('account', 'url', $config);
        $this->substituteConfig('account_locator', 'url', $config);
        $this->substituteConfig('database', 'url', $config);
        $this->substituteConfig('schema', 'url', $config);
        $this->substituteConfig('warehouse', 'url', $config);
        $this->substituteConfig('username', 'url', $config);
        $this->substituteConfig('password', 'url', $config);
        // key/passcode are intentionally NOT overridable via URL query params —
        // key material must not land in URLs (logs, history, referrers). They
        // are still overridable via headers; checkHeaders() handles that.
        $this->substituteConfig('role', 'url', $config);
    }

    protected function substituteConfig($name, $parameter, &$config)
    {
        switch ($parameter) {
            case 'header':
            {
                if (request()->hasHeader($name) && !empty(request()->header($name))) {
                    $config[$name] = request()->header($name);
                }
                break;
            }
            case 'url':
            {
                if (request()->has($name) && !empty(request()->query($name))) {
                    $config[$name] = request()->query($name);
                }
                break;
            }
        }
    }
}
