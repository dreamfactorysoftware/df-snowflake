<?php
namespace DreamFactory\Core\Skeleton;

use DreamFactory\Core\Compliance\Handlers\Events\EventHandler;
use DreamFactory\Core\Skeleton\Http\Middleware\ExampleMiddleware;
use DreamFactory\Core\Skeleton\Models\ExampleConfig;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Skeleton\Services\ExampleService;
use Illuminate\Routing\Router;

use Route;
use Event;


class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function boot()
    {
        // add migrations
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');

        Event::subscribe(new EventHandler());

        // add routes
        /** @noinspection PhpUndefinedMethodInspection */
        if (!$this->app->routesAreCached()) {
            include '/opt/dreamfactory/vendor/dreamfactory/df-skeleton/routes/routes.php';
        }

        $this->addMiddleware();
    }

    public function register()
    {

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'example',
                    'label'           => 'Example Service',
                    'description'     => 'Example of new service.',
                    'group'           => 'New connector group', // or if you want to use defined groups use DreamFactory\Core\Enums\ServiceTypeGroups, ServiceTypeGroups::REMOTE
                    'subscription_required' => LicenseLevel::GOLD, // don't specify this if you want the service be used on Open Source version
                    'config_handler'  => ExampleConfig::class,
                    'factory'         => function ($config) {
                        return new ExampleService($config);
                    },
                ])
            );
        });
    }

    /**
     * Register any middleware aliases.
     *
     * @return void
     */
    protected function addMiddleware()
    {
        // the method name was changed in Laravel 5.4
        if (method_exists(Router::class, 'aliasMiddleware')) {
            Route::aliasMiddleware('df.example_middleware', ExampleMiddleware::class);
        } else {
            /** @noinspection PhpUndefinedMethodInspection */
            Route::middleware('df.example_middleware', ExampleMiddleware::class);
        }

        Route::pushMiddlewareToGroup('df.api', 'df.example_middleware');
    }
}
