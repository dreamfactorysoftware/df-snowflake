<?php
namespace DreamFactory\Core\Snowflake;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Snowflake\Database\Connectors\SnowflakeConnector;
use DreamFactory\Core\Snowflake\Database\Schema\SnowflakeSchema;
use DreamFactory\Core\Snowflake\Database\SnowflakeConnection;
use DreamFactory\Core\Snowflake\Models\SnowflakeDbConfig;
use DreamFactory\Core\Snowflake\Services\SnowflakeDb;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {

        $this->app->resolving('df.db.schema', function (DbSchemaExtensions $db){
            $db->extend('snowflake', function ($connection){
                return new SnowflakeSchema($connection);
            });
        });

        $this->app->resolving('db', function (DatabaseManager $db){
            $db->extend('snowflake', function ($config){
                $this->checkUrlParams($config['config']);
                $this->checkHeaders($config['config']);
                $connector = new SnowflakeConnector();
                $connection = $connector->connect($config);

                return new SnowflakeConnection($connection, $config['database'], '', $config);
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


    protected function checkHeaders(&$config)
    {
        $this->substituteConfig('hostname', 'header', $config);
        $this->substituteConfig('account', 'header', $config);
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
        $this->substituteConfig('database', 'url', $config);
        $this->substituteConfig('schema', 'url', $config);
        $this->substituteConfig('warehouse', 'url', $config);
        $this->substituteConfig('username', 'url', $config);
        $this->substituteConfig('password', 'url', $config);
        $this->substituteConfig('key', 'header', $config);
        $this->substituteConfig('passcode', 'header', $config);
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
