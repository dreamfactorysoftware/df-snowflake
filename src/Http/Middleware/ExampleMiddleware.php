<?php


namespace DreamFactory\Core\Skeleton\Http\Middleware;

use Closure;

class ExampleMiddleware
{
    private $method;
    private $request;

    /**
     * @param         $request
     * @param Closure $next
     *
     * @return mixed
     * @throws \Exception
     */
    function handle($request, Closure $next)
    {
        $this->request = $request;
        $this->method = $request->getMethod();
        $response = $next($request);

        if ($response->isSuccessful())
        {
            $content = $response->getOriginalContent();
            $content['middleware_on'] = true;
            $response->setContent($content);
        }

        return $response;
    }
}