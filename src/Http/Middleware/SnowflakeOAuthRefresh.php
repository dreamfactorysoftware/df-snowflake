<?php

namespace DreamFactory\Core\Snowflake\Http\Middleware;

use Closure;
use DreamFactory\Core\Models\Service;
use Illuminate\Support\Facades\Http;

/**
 * Automatically refresh Snowflake OAuth tokens before they expire
 */
class SnowflakeOAuthRefresh
{
    /**
     * Handle an incoming request.
     *
     * @param  \Illuminate\Http\Request  $request
     * @param  \Closure  $next
     * @return mixed
     */
    public function handle($request, Closure $next)
    {
        // Check if this is a Snowflake service request
        $serviceName = $request->route('service');

        if (!$serviceName) {
            return $next($request);
        }

        $service = Service::whereName($serviceName)->first();

        if (!$service || $service->type !== 'snowflake') {
            return $next($request);
        }

        $config = $service->config;

        // Check if using OAuth
        if (empty($config['authenticator']) || $config['authenticator'] !== 'oauth') {
            return $next($request);
        }

        // Check if token needs refresh (refresh 5 minutes before expiry)
        $expiresAt = $config['oauth_token_expires_at'] ?? null;
        if (!$expiresAt) {
            return $next($request);
        }

        $expiryTime = \Carbon\Carbon::parse($expiresAt);
        $shouldRefresh = now()->addMinutes(5)->isAfter($expiryTime);

        if ($shouldRefresh) {
            try {
                $this->refreshToken($service);
                \Log::info('Auto-refreshed Snowflake OAuth token', ['service' => $serviceName]);
            } catch (\Exception $e) {
                \Log::error('Failed to auto-refresh Snowflake OAuth token', [
                    'service' => $serviceName,
                    'error' => $e->getMessage()
                ]);
                // Continue anyway - let the request try with existing token
            }
        }

        return $next($request);
    }

    /**
     * Refresh OAuth token
     */
    protected function refreshToken(Service $service)
    {
        $config = $service->config;
        $refreshToken = $config['oauth_refresh_token'] ?? null;

        if (!$refreshToken) {
            throw new \Exception('No refresh token available');
        }

        $clientId = $config['oauth_client_id'];
        $clientSecret = $config['oauth_client_secret'];
        $account = $config['account'];

        $tokenUrl = "https://{$account}.snowflakecomputing.com/oauth/token-request";

        $response = Http::asForm()->post($tokenUrl, [
            'grant_type' => 'refresh_token',
            'refresh_token' => $refreshToken,
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
        ]);

        if (!$response->successful()) {
            throw new \Exception('Failed to refresh token: ' . $response->body());
        }

        $tokens = $response->json();

        // Update tokens
        $config['oauth_access_token'] = $tokens['access_token'];
        if (isset($tokens['refresh_token'])) {
            $config['oauth_refresh_token'] = $tokens['refresh_token'];
        }
        $config['oauth_token_expires_at'] = now()->addSeconds($tokens['expires_in'])->toDateTimeString();

        $service->config = $config;
        $service->save();
    }
}
