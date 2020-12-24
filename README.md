**This guide is not a strict instruction. You don't have to follow every part of it.**

## DreamFactory Skeleton

> **Note:** This repository contains the code needed to create a new connector of the DreamFactory platform. 
If you want the full DreamFactory platform, visit the 
main [DreamFactory repository](https://github.com/dreamfactorysoftware/dreamfactory).

You are free to integrate anything [Laravel](https://laravel.com/docs/5.6/) provides, note that DreamFactory uses Laravel 5.6 version.
 
You can clone content of this repository to your own to get new functional connector of DreamFactory Platform.
 
To connect it to DF just add the following to the require section of 
[main composer](https://github.com/dreamfactorysoftware/dreamfactory/blob/ce72cc6739979be286f51617050bc9ec9c657f39/composer.json#L30):
```
"require":     {
    "dreamfactory/df-skeleton":   "~0.1.0"    // instead of skeleton write name of your package
}
``` 

Then run `composer require dreamfactory/df-skeleton update--no-dev --ignore-platform-reqs `

### Documentation

Documentation for the platform can be found on the [DreamFactory wiki](http://wiki.dreamfactory.com).

# Create a new DreamFactory connector

First of all, your package composer need to require df-core 
and, in case of creating system service, df-system. 
Just add the following to the composer.json file of your package:
```json
"require":     {
    "dreamfactory/df-core":   "~0.15.1",
    "dreamfactory/df-system": "~0.2.0"
}
```

# This package has: 

### Folders

- `./database` 

Things that concern database. For example [migrations](https://laravel.com/docs/5.6/migrations), if your package needs a table in system database.

- `./routes`

If you need a non DF API [route](https://laravel.com/docs/5.6/routing), you can describe it in routes.php file.

- `./src/Components`

Any support classes, like wrappers or classes that implement the 3rd party packages functionality.

- `./src/Enum`

In some cases you want to define a constant list of values. This is a food place to place them.

- `./src/Events`

Your package can define [events](https://laravel.com/docs/5.6/events) to subscribe to them later.

- `./src/Handlers/Events`

Define event handlers or any other here.

- `./src/Http` 
Things that handle routes from `./routes` folder.

  - `./src/Http/Middleware`
  
  Requests middleware should go here.
  
  - `./src/Http/Conrollers`
  
  Contollers of `./routes` should go here.
    
- `./src/Models`

Define Eloquent [Models](https://laravel.com/docs/5.8/eloquent) here.

- `./src/Resources`

API [Resources](https://laravel.com/docs/5.6/eloquent-resources). It may extend `DreamFactory\Core\Resources\BaseRestResource` from df-core.

- `./src/Services`

Define your new Service here. It may also extend `DreamFactory\Core\Services\BaseRestService` or any other base services from df-core.

- `./src/Testing`

PHPUnit tests go here.

### Classes:

1. Service that extends `DreamFactory\Core\Services\BaseRestService` (__see [src/Services](https://github.com/dreamfactorysoftware/df-skeleton/blob/add-examples/src/Services/ExampleService.php)__)
2. Database table that describes your connector config (__see [database/migrations](https://github.com/dreamfactorysoftware/df-skeleton/blob/add-examples/database/migrations/2019_08_12_125323_create_example_table.php)__)
3. Model that connects to the table and extends `DreamFactory\Core\Models\BaseServiceConfigModel` (__see [src/Models](https://github.com/dreamfactorysoftware/df-skeleton/blob/master/src/Models/ExampleModel.php)__).
There is an opportunity to create connector not using database (__see [DreamFactory\Core\Models\BaseServiceConfigNoDbModel](https://github.com/dreamfactorysoftware/df-core/blob/master/src/Models/BaseServiceConfigNoDbModel.php)__)

4. Resource that extends `DreamFactory\Core\Resources\BaseRestResource` (__see [src/Resources](https://github.com/dreamfactorysoftware/df-skeleton/blob/add-examples/src/Resources/ExampleResource.php)__)


For parent classes you can override methods to satisfy your needs. 

In your resource and service you can override handleGET, handlePOST methods from [df-core](https://github.com/dreamfactorysoftware/df-core/blob/06e01cd46ed106684041fb1fdf8ef35695a1b2cf/src/Components/RestHandler.php#L589) to determine responses (only if 
[$autoDispatch = true;](https://github.com/dreamfactorysoftware/df-core/blob/06e01cd46ed106684041fb1fdf8ef35695a1b2cf/src/Components/RestHandler.php#L88)).

[ServiceProvider](https://github.com/dreamfactorysoftware/df-skeleton/blob/master/src/ServiceProvider.php) connects the package to the application.

*Remember you are not limited and you can implement anything Laravel provide.*