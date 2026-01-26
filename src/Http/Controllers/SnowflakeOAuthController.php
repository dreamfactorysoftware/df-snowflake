<?php

namespace DreamFactory\Core\Snowflake\Http\Controllers;

use DreamFactory\Core\Models\Service;
use DreamFactory\Core\Snowflake\Utility\SnowflakeNativeAppDetector;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;

/**
 * Handles Snowflake OAuth authorization flow
 */
class SnowflakeOAuthController extends Controller
{
    /**
     * Initiate OAuth authorization flow
     *
     * GET /api/v2/snowflake/oauth/authorize?service_id=123
     */
    public function authorize(Request $request)
    {
        $serviceId = $request->input('service_id');

        if (!$serviceId) {
            return response()->json(['error' => 'service_id required'], 400);
        }

        $service = Service::find($serviceId);
        if (!$service || $service->type !== 'snowflake') {
            return response()->json(['error' => 'Invalid Snowflake service'], 404);
        }

        // Check if running in SPCS (Snowpark Container Services)
        $spcsTokenPath = '/snowflake/session/token';
        if (file_exists($spcsTokenPath)) {
            \Log::info('SPCS environment detected in authorize(), using session token directly');

            try {
                // Read the SPCS session token
                $accessToken = trim(file_get_contents($spcsTokenPath));

                if (empty($accessToken)) {
                    throw new \Exception('SPCS session token file is empty');
                }

                \Log::info('Successfully read SPCS session token in authorize()', [
                    'token_length' => strlen($accessToken),
                    'token_prefix' => substr($accessToken, 0, 20) . '...'
                ]);

                // Update service config with SPCS token
                $config = $service->config;
                $config['oauth_access_token'] = $accessToken;
                // SPCS tokens don't expire in the traditional sense, set far future
                $config['oauth_token_expires_at'] = now()->addYears(10)->toDateTimeString();
                // No refresh token needed for SPCS
                $config['oauth_refresh_token'] = null;

                // Enable ODBC mode and set authenticator
                $config['use_odbc'] = true;
                $config['authenticator'] = 'oauth';

                $service->config = $config;
                $service->save();

                return response()->json([
                    'success' => true,
                    'message' => 'OAuth authorization successful using SPCS session token.',
                    'spcs_mode' => true,
                    'expires_at' => $config['oauth_token_expires_at'],
                    'direct_auth' => true
                ]);

            } catch (\Exception $e) {
                \Log::error('Failed to use SPCS session token in authorize(): ' . $e->getMessage());
                return response()->json(['error' => 'Failed to read SPCS session token: ' . $e->getMessage()], 500);
            }
        }

        // Standard OAuth flow for non-SPCS environments
        $config = $service->config;
        $clientId = $config['oauth_client_id'] ?? null;
        // Use account_locator for OAuth URLs, fall back to account if not set
        $accountForOAuth = $config['account_locator'] ?? $config['account'] ?? null;
        // Convert to lowercase for OAuth URLs
        $accountForOAuth = strtolower($accountForOAuth);

        // Only require account - client_id may be auto-configured in native app
        if (!$accountForOAuth) {
            return response()->json(['error' => 'OAuth not configured. Set account_locator (or account).'], 400);
        }

        // If no client_id, this is likely a native app deployment
        if (!$clientId) {
            return response()->json([
                'success' => true,
                'message' => 'OAuth will be auto-configured when deployed to Snowflake Native App (SPCS). No client_id required.',
                'native_app_mode' => true,
                'account' => $accountForOAuth
            ]);
        }

        // Build authorization URL
        $redirectUri = url("/api/v2/_oauth/snowflake/callback");
        $state = base64_encode(json_encode(['service_id' => $serviceId]));

        // Build scope - use specific role if configured, otherwise use session:role-any
        $role = $config['role'] ?? 'PUBLIC';
        $scope = "session:role:{$role}";

        $authUrl = "https://{$accountForOAuth}.snowflakecomputing.com/oauth/authorize?" . http_build_query([
            'client_id' => $clientId,
            'response_type' => 'code',
            'redirect_uri' => $redirectUri,
            'state' => $state,
            'scope' => $scope
        ]);

        // Return redirect URL (frontend will redirect user)
        return response()->json([
            'authorization_url' => $authUrl,
            'message' => 'Redirect user to authorization_url to complete OAuth flow'
        ]);
    }

    /**
     * Handle OAuth callback from Snowflake
     *
     * GET /api/v2/snowflake/oauth/callback?code=xxx&state=xxx
     */
    public function callback(Request $request)
    {
        $code = $request->input('code');
        $stateParam = $request->input('state');

        if (!$code || !$stateParam) {
            return response()->json(['error' => 'Missing authorization code or state'], 400);
        }

        // Decode state
        $state = json_decode(base64_decode($stateParam), true);
        $serviceId = $state['service_id'] ?? null;

        if (!$serviceId) {
            return response()->json(['error' => 'Invalid state parameter'], 400);
        }

        $service = Service::find($serviceId);
        if (!$service) {
            return response()->json(['error' => 'Service not found'], 404);
        }

        $config = $service->config;

        // Check if running in SPCS (Snowpark Container Services)
        $spcsTokenPath = '/snowflake/session/token';
        if (file_exists($spcsTokenPath)) {
            \Log::info('SPCS environment detected, using session token from ' . $spcsTokenPath);

            try {
                // Read the SPCS session token
                $accessToken = trim(file_get_contents($spcsTokenPath));

                if (empty($accessToken)) {
                    throw new \Exception('SPCS session token file is empty');
                }

                \Log::info('Successfully read SPCS session token', [
                    'token_length' => strlen($accessToken),
                    'token_prefix' => substr($accessToken, 0, 20) . '...'
                ]);

                // Update service config with SPCS token
                $config['oauth_access_token'] = $accessToken;
                // SPCS tokens don't expire in the traditional sense, set far future
                $config['oauth_token_expires_at'] = now()->addYears(10)->toDateTimeString();
                // No refresh token needed for SPCS
                $config['oauth_refresh_token'] = null;

                // Enable ODBC mode and set authenticator
                $config['use_odbc'] = true;
                $config['authenticator'] = 'oauth';

                $service->config = $config;
                $service->save();

                return response()->json([
                    'success' => true,
                    'message' => 'OAuth authorization successful using SPCS session token.',
                    'spcs_mode' => true,
                    'expires_at' => $config['oauth_token_expires_at']
                ]);

            } catch (\Exception $e) {
                \Log::error('Failed to use SPCS session token: ' . $e->getMessage());
                return response()->json(['error' => 'Failed to read SPCS session token: ' . $e->getMessage()], 500);
            }
        }

        // Standard OAuth flow for non-SPCS environments
        $clientId = $config['oauth_client_id'];
        // Use raw (unencrypted) secret to avoid truncation bug
        $clientSecret = $config['oauth_client_secret_raw'] ?? $config['oauth_client_secret'];
        // Use account_locator for OAuth URLs, fall back to account if not set
        $accountForOAuth = $config['account_locator'] ?? $config['account'];
        // Convert to lowercase for OAuth URLs
        $accountForOAuth = strtolower($accountForOAuth);
        $redirectUri = url("/api/v2/_oauth/snowflake/callback");

        // Exchange code for tokens
        try {
            $tokenUrl = "https://{$accountForOAuth}.snowflakecomputing.com/oauth/token-request";

            \Log::info('Attempting OAuth token exchange', [
                'token_url' => $tokenUrl,
                'client_id' => $clientId,
                'client_secret_length' => strlen($clientSecret),
                'client_secret_first_4' => substr($clientSecret, 0, 4),
                'redirect_uri' => $redirectUri,
                'code_length' => strlen($code),
                'has_client_secret' => !empty($clientSecret)
            ]);

            // Use HTTP Basic Auth for client credentials (RFC 6749)
            $response = \Http::withBasicAuth($clientId, $clientSecret)
                ->asForm()
                ->post($tokenUrl, [
                    'grant_type' => 'authorization_code',
                    'code' => $code,
                    'redirect_uri' => $redirectUri,
                ]);

            if (!$response->successful()) {
                \Log::error('Snowflake OAuth token exchange failed', [
                    'status' => $response->status(),
                    'body' => $response->body(),
                    'client_id_used' => $clientId
                ]);
                return response()->json(['error' => 'Failed to exchange authorization code'], 500);
            }

            $tokens = $response->json();

            // Update service config with tokens
            $config['oauth_access_token'] = $tokens['access_token'];
            $config['oauth_refresh_token'] = $tokens['refresh_token'] ?? null;
            $config['oauth_token_expires_at'] = now()->addSeconds($tokens['expires_in'])->toDateTimeString();

            // Enable ODBC mode and set authenticator
            $config['use_odbc'] = true;
            $config['authenticator'] = 'oauth';

            $service->config = $config;
            $service->save();

            return response()->json([
                'success' => true,
                'message' => 'OAuth authorization successful. Service configured.',
                'expires_at' => $config['oauth_token_expires_at']
            ]);

        } catch (\Exception $e) {
            \Log::error('Snowflake OAuth callback error: ' . $e->getMessage());
            return response()->json(['error' => 'OAuth flow failed: ' . $e->getMessage()], 500);
        }
    }

    /**
     * Manually refresh OAuth token
     *
     * POST /api/v2/snowflake/oauth/refresh?service_id=123
     */
    public function refresh(Request $request)
    {
        $serviceId = $request->input('service_id');

        if (!$serviceId) {
            return response()->json(['error' => 'service_id required'], 400);
        }

        $service = Service::find($serviceId);
        if (!$service) {
            return response()->json(['error' => 'Service not found'], 404);
        }

        try {
            $this->refreshToken($service);

            return response()->json([
                'success' => true,
                'message' => 'Token refreshed successfully',
                'expires_at' => $service->config['oauth_token_expires_at']
            ]);
        } catch (\Exception $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }

    /**
     * Refresh OAuth token using refresh token
     */
    protected function refreshToken(Service $service)
    {
        $config = $service->config;

        // Check if running in SPCS - re-read the session token
        $spcsTokenPath = '/snowflake/session/token';
        if (file_exists($spcsTokenPath)) {
            \Log::info('SPCS environment detected, refreshing token from ' . $spcsTokenPath);

            try {
                $accessToken = trim(file_get_contents($spcsTokenPath));

                if (empty($accessToken)) {
                    throw new \Exception('SPCS session token file is empty');
                }

                // Update with fresh SPCS token
                $config['oauth_access_token'] = $accessToken;
                $config['oauth_token_expires_at'] = now()->addYears(10)->toDateTimeString();

                $service->config = $config;
                $service->save();

                \Log::info('SPCS token refreshed successfully', ['service_id' => $service->id]);
                return;

            } catch (\Exception $e) {
                \Log::error('Failed to refresh SPCS token: ' . $e->getMessage());
                throw new \Exception('Failed to refresh SPCS session token: ' . $e->getMessage());
            }
        }

        // Standard OAuth refresh for non-SPCS environments
        $refreshToken = $config['oauth_refresh_token'] ?? null;

        if (!$refreshToken) {
            throw new \Exception('No refresh token available');
        }

        $clientId = $config['oauth_client_id'];
        // Use raw (unencrypted) secret to avoid truncation bug
        $clientSecret = $config['oauth_client_secret_raw'] ?? $config['oauth_client_secret'];
        // Use account_locator for OAuth URLs, fall back to account if not set
        $accountForOAuth = $config['account_locator'] ?? $config['account'];
        // Convert to lowercase for OAuth URLs
        $accountForOAuth = strtolower($accountForOAuth);

        $tokenUrl = "https://{$accountForOAuth}.snowflakecomputing.com/oauth/token-request";

        // Use HTTP Basic Auth for client credentials (RFC 6749)
        $response = \Http::withBasicAuth($clientId, $clientSecret)
            ->asForm()
            ->post($tokenUrl, [
                'grant_type' => 'refresh_token',
                'refresh_token' => $refreshToken,
            ]);

        if (!$response->successful()) {
            throw new \Exception('Failed to refresh token: ' . $response->body());
        }

        $tokens = $response->json();

        // Update tokens in config
        $config['oauth_access_token'] = $tokens['access_token'];
        if (isset($tokens['refresh_token'])) {
            $config['oauth_refresh_token'] = $tokens['refresh_token'];
        }
        $config['oauth_token_expires_at'] = now()->addSeconds($tokens['expires_in'])->toDateTimeString();

        $service->config = $config;
        $service->save();

        \Log::info('Snowflake OAuth token refreshed', ['service_id' => $service->id]);
    }

    /**
     * Get OAuth status for a service
     *
     * GET /api/v2/snowflake/oauth/status?service_id=123
     */
    public function status(Request $request)
    {
        $serviceId = $request->input('service_id');

        if (!$serviceId) {
            return response()->json(['error' => 'service_id required'], 400);
        }

        $service = Service::find($serviceId);
        if (!$service) {
            return response()->json(['error' => 'Service not found'], 404);
        }

        $config = $service->config;
        $hasToken = !empty($config['oauth_access_token']);
        $expiresAt = $config['oauth_token_expires_at'] ?? null;
        $isExpired = $expiresAt ? now()->isAfter($expiresAt) : true;

        // Check for native app mode
        $isNativeApp = SnowflakeNativeAppDetector::isNativeApp();
        $spcsTokenExists = file_exists('/snowflake/session/token');

        return response()->json([
            'configured' => !empty($config['oauth_client_id']),
            'authorized' => $hasToken,
            'expires_at' => $expiresAt,
            'is_expired' => $isExpired,
            'needs_refresh' => $hasToken && $isExpired,
            'is_native_app' => $isNativeApp,
            'spcs_token_available' => $spcsTokenExists
        ]);
    }

}
