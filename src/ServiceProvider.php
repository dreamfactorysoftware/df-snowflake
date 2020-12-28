<?php
namespace DreamFactory\Core\Snowflake;

use DreamFactory\Core\Snowflake\Models\SnowflakeDbConfig;
use DreamFactory\Core\Snowflake\Services\SnowflakeDb;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {

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
