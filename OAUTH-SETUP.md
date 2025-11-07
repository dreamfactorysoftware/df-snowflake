# Snowflake OAuth Setup Guide

This guide walks you through setting up OAuth authentication for Snowflake in DreamFactory.

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- DreamFactory instance accessible via HTTPS (OAuth requires HTTPS)
- df-snowflake package with ODBC support installed

## Step 1: Create OAuth Integration in Snowflake

Connect to Snowflake as ACCOUNTADMIN and run:

```sql
-- Create OAuth security integration
CREATE SECURITY INTEGRATION dreamfactory_oauth
  TYPE = OAUTH
  ENABLED = TRUE
  OAUTH_CLIENT = CUSTOM
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://YOUR-DREAMFACTORY-DOMAIN/api/v2/_oauth/snowflake/callback'
  OAUTH_ISSUE_REFRESH_TOKENS = TRUE
  OAUTH_REFRESH_TOKEN_VALIDITY = 7776000; -- 90 days

-- Get the Client ID and Secret
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('DREAMFACTORY_OAUTH');
```

The output will look like:
```json
{
  "OAUTH_CLIENT_ID": "abc123...",
  "OAUTH_CLIENT_SECRET": "secret1...",
  "OAUTH_CLIENT_SECRET_2": "secret2..."
}
```

**Important Notes**:
- Replace `YOUR-DREAMFACTORY-DOMAIN` with your actual DreamFactory domain (e.g., `api.mycompany.com`)
- The path `/api/v2/_oauth/snowflake/callback` is FIXED - do not change it
- The `_oauth` prefix prevents collision with services named "snowflake"
- This redirect URI is the same for ALL Snowflake services in your DreamFactory instance

**OAuth Credentials to Save**:
- **Client ID**: Use the value from `OAUTH_CLIENT_ID`
- **Client Secret**: Use the value from `OAUTH_CLIENT_SECRET` (either secret works, but use the first one)

## Step 2: Create Snowflake Service in DreamFactory

1. Log into DreamFactory admin interface
2. Go to **Services** → **Create**
3. Select **Snowflake** as service type
4. Configure basic settings:
   - **Name**: `my_snowflake`
   - **Account**: Your Snowflake account identifier (e.g., `UCZWIRU-JUB93638`)
   - **Database**: Database name
   - **Schema**: Schema name
   - **Warehouse**: Warehouse name
   - **Role**: Role to use (e.g., `PUBLIC`)

5. Add OAuth credentials:
   - **OAuth Client ID**: (paste the `OAUTH_CLIENT_ID` value from Step 1)
   - **OAuth Client Secret**: (paste the `OAUTH_CLIENT_SECRET` value from Step 1)
   - **Authenticator**: Select `oauth`
   - **Use ODBC**: Check this box

6. Click **Save** (don't test connection yet - no token)

## Step 3: Authorize with Snowflake

### Via API

```bash
# Step 1: Login to get session token
SESSION_RESPONSE=$(curl -X POST "http://165.232.137.171/api/v2/system/admin/session" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"yourpassword"}')

SESSION_TOKEN=$(echo $SESSION_RESPONSE | jq -r '.session_token')

# Step 2: Get authorization URL (use your service ID, e.g., 8)
AUTH_RESPONSE=$(curl -X GET "http://165.232.137.171/api/v2/snowflake/oauth/authorize?service_id=8" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY" \
  -H "X-DreamFactory-Session-Token: $SESSION_TOKEN")

AUTH_URL=$(echo $AUTH_RESPONSE | jq -r '.authorization_url')
echo "Open this URL in your browser:"
echo $AUTH_URL

# Step 3: Open the URL in a browser, log into Snowflake, grant permission
# Snowflake will redirect back to DreamFactory and save the tokens automatically

# Step 4: Verify authorization
curl -X GET "http://165.232.137.171/api/v2/snowflake/oauth/status?service_id=8" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY" \
  -H "X-DreamFactory-Session-Token: $SESSION_TOKEN"
```

## Step 4: Verify OAuth Status

```bash
curl -X GET "https://your-df-instance/api/v2/snowflake/oauth/status?service_id=$SERVICE_ID" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY"
```

Expected response:
```json
{
  "configured": true,
  "authorized": true,
  "expires_at": "2025-10-03 12:00:00",
  "is_expired": false,
  "needs_refresh": false
}
```

## Step 5: Use the Service

Now you can make API calls to your Snowflake service:

```bash
# List tables
curl -X GET "https://your-df-instance/api/v2/my_snowflake/_table" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY"

# Query a table
curl -X GET "https://your-df-instance/api/v2/my_snowflake/_table/CUSTOMERS" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY"
```

## Automatic Token Refresh

Tokens are automatically refreshed 5 minutes before expiration. No manual intervention needed!

## Manual Token Refresh

If needed, you can manually refresh tokens:

```bash
curl -X POST "https://your-df-instance/api/v2/snowflake/oauth/refresh?service_id=$SERVICE_ID" \
  -H "X-DreamFactory-API-Key: YOUR_API_KEY"
```

## Troubleshooting

### "OAuth not configured" Error

Make sure you've set:
- `oauth_client_id`
- `oauth_client_secret`
- `account`
- `authenticator` = `oauth`
- `use_odbc` = `true`

### "Failed to exchange authorization code"

Check:
- Redirect URI in Snowflake integration matches exactly: `https://your-domain/api/v2/snowflake/oauth/callback`
- Client ID and Secret are correct
- Snowflake integration is ENABLED

### Token Expired

If a token expires and can't be refreshed:
1. Check refresh token validity in Snowflake integration
2. Re-authorize using Step 3

### Memory Issues

If you encounter memory errors:
- Increase PHP `memory_limit` to at least 512M
- See `ODBC-README.md` for memory optimization details

## Security Best Practices

1. **Use HTTPS** - OAuth requires secure redirect URIs
2. **Protect Client Secret** - Store securely, never commit to version control
3. **Limit Scope** - Only request minimum required permissions
4. **Monitor Token Usage** - Check logs for failed refresh attempts
5. **Rotate Secrets** - Periodically regenerate Client ID/Secret in Snowflake

## For Developers

### OAuth Flow Endpoints

- **GET** `/api/v2/snowflake/oauth/authorize?service_id={id}` - Start authorization
- **GET** `/api/v2/snowflake/oauth/callback?code=xxx&state=xxx` - OAuth callback (automatic)
- **POST** `/api/v2/snowflake/oauth/refresh?service_id={id}` - Manual refresh
- **GET** `/api/v2/snowflake/oauth/status?service_id={id}` - Check token status

### Adding UI Integration

To add an "Authorize with Snowflake" button in the admin interface, you would:

1. Detect when `authenticator` = `oauth` and no `oauth_access_token` exists
2. Show button that calls `/api/v2/snowflake/oauth/authorize`
3. Open returned `authorization_url` in popup or new tab
4. Poll `/api/v2/snowflake/oauth/status` until `authorized` = `true`
5. Show success message

See `src/Http/Controllers/SnowflakeOAuthController.php` for implementation details.
