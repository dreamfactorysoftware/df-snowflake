<?php

/*
|--------------------------------------------------------------------------
| Skeleton Service Routes
|--------------------------------------------------------------------------
|
| These routes created in example purposes.
|
*/

Route::prefix(config('df.example_prefix'))
    ->middleware('df.cors')
    ->group(function () {
        $resourcePattern = '[0-9a-zA-Z-_@&\#\!=,:;\/\^\$\.\|\{\}\[\]\(\)\*\+\? ]+';
        $servicePattern = '[_0-9a-zA-Z-.]+';
        $controller = 'DreamFactory\Core\Skeleton\Http\Controllers\ExampleController';

        Route::get('example-route', $controller . '@index');

        /*
        Route::get('{example}/{path}', $controller . '@streamFile')->where(
            ['example' => $servicePattern, 'path' => $resourcePattern]
        );
        */
    }
    );