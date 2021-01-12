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

        $this->app->resolving('db.schema', function (DbSchemaExtensions $db){
            $db->extend('snowflake', function ($connection){
                return new SnowflakeSchema($connection);
            });
        });

        $this->app->resolving('db', function (DatabaseManager $db){
            $db->extend('snowflake', function ($config){
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
                    'group'           => 'Big Data', // or if you want to use defined groups use DreamFactory\Core\Enums\ServiceTypeGroups, ServiceTypeGroups::REMOTE
                    'subscription_required' => LicenseLevel::GOLD, // don't specify this if you want the service be used on Open Source version
                    'config_handler'  => SnowflakeDbConfig::class,
                    'factory'         => function ($config) {
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
}
