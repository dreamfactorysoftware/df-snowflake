<?php namespace DreamFactory\Core\Skeleton\Services;

use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Models\Service;
use DreamFactory\Core\Services\BaseRestService;
use DreamFactory\Core\Skeleton\Components\ExampleComponent;
use DreamFactory\Core\Skeleton\Models\ExampleConfig;
use DreamFactory\Core\Skeleton\Resources\ExampleResource;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;

class ExampleService extends BaseRestService
{
    /**
     * @var \DreamFactory\Core\Skeleton\Models\ExampleConfig
     */
    protected $exampleModel = null;


    //*************************************************************************
    //* Methods
    //*************************************************************************

    /**
     * Create a new ExampleService
     *
     * Create your methods, properties or override ones from the parent
     *
     * @param array $settings settings array
     *
     * @throws \InvalidArgumentException
     */
    public function __construct($settings)
    {
        $this->exampleModel = new ExampleConfig();
        parent::__construct($settings);
    }

    /**
     * Fetches example.
     *
     * @return array
     * @throws UnauthorizedException
     */
    protected function handleGET()
    {
        $user = Session::user();

        if (empty($user)) {
            throw new UnauthorizedException('There is no valid session for the current request.');
        }

        if(ExampleComponent::getExample() !== "example"){
            throw new BadRequestException('Something went wrong in Skeleton Component');
        }

        $content = $this->exampleModel->all();

        return ["message" => "You sent a GET request to DF " . $this->getName() . " service!",
            "content" => $content];
    }

    /**
     * Updates user profile.
     *
     * @return array
     * @throws NotFoundException
     * @throws \Exception
     */
    protected function handlePOST()
    {
        $user = Session::user();

        if (empty($user)) {
            throw new NotFoundException('No user session found.');
        }
        return ["You sent a POST request to " . $this->getName() . " DF service!"];
    }

    /** @type array Service Resources
     *  The resource url would be /api/v2/{service_name}/example
     */
    protected static $resources = [
        ExampleResource::RESOURCE_NAME => [
            'name'       => ExampleResource::RESOURCE_NAME,
            'class_name' => ExampleResource::class,
            'label'      => 'Example'
        ]
    ];
}
