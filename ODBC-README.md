# Snowflake ODBC Connector for DreamFactory

This branch (`feature/snowflake-odbc-oauth`) adds ODBC-based connectivity to the DreamFactory Snowflake connector, enabling OAuth 2.0 authentication and other advanced authentication methods.

## Features

- **Dual Mode Support**: Maintains backward compatibility with PDO while adding ODBC support
- **Multiple Authentication Methods**:
  - Username/Password (PDO or ODBC)
  - Key-Pair JWT Authentication (PDO or ODBC)
  - **OAuth 2.0** (ODBC only)
  - **External Browser SSO** (ODBC only)
- **Automatic Driver Selection**: Automatically uses ODBC when OAuth is configured
- **API Compatibility**: Maintains full API compatibility with existing Snowflake services

## Architecture

### New Components

1. **SnowflakeOdbcConnector** (`src/Database/Connectors/SnowflakeOdbcConnector.php`)
   - Handles ODBC connection establishment
   - Builds ODBC DSN strings for different auth methods
   - Manages OAuth token injection

2. **SnowflakeOdbcConnection** (`src/Database/SnowflakeOdbcConnection.php`)
   - Extends Laravel's Connection class
   - Translates PDO-style queries to ODBC
   - Maintains compatibility with existing query builders

3. **Enhanced SnowflakeDbConfig** (`src/Models/SnowflakeDbConfig.php`)
   - Added OAuth configuration fields
   - Added `use_odbc` flag for manual driver selection
   - Added `authenticator` field for auth method selection

4. **Updated ServiceProvider** (`src/ServiceProvider.php`)
   - Automatically selects PDO or ODBC based on configuration
   - Routes to appropriate connector and connection classes

## Docker Setup

The Docker environment includes:
- Snowflake ODBC driver 3.5.0
- PHP ODBC extension
- Configured ODBC ini files

### ODBC Configuration Files

**odbcinst.ini**:
```ini
[Snowflake]
Description=Snowflake ODBC Driver
Driver=/usr/lib/snowflake/odbc/lib/libSnowflake.so
```

**odbc.ini**:
```ini
[ODBC]
Trace=no

[ODBC Data Sources]
Snowflake=Snowflake ODBC Driver
```

## Configuration

### DreamFactory Service Configuration

When creating/editing a Snowflake service in DreamFactory, the following fields are now available:

| Field | Type | Description |
|-------|------|-------------|
| `use_odbc` | Boolean | Enable ODBC driver (auto-enabled for OAuth) |
| `authenticator` | Picklist | Authentication method: `snowflake`, `snowflake_jwt`, `oauth`, `externalbrowser` |
| `oauth_client_id` | String | OAuth 2.0 Client ID |
| `oauth_client_secret` | Password | OAuth 2.0 Client Secret |
| `oauth_access_token` | Password | Current access token (auto-populated) |
| `oauth_refresh_token` | Password | Refresh token (auto-populated) |
| `oauth_token_expires_at` | String | Token expiration timestamp |

### Authentication Method Selection

**Username/Password** (PDO - default):
```json
{
  "authenticator": "snowflake",
  "account": "myaccount",
  "username": "myuser",
  "password": "mypass",
  "database": "mydb",
  "schema": "myschema",
  "warehouse": "mywh"
}
```

**Key-Pair JWT** (PDO or ODBC):
```json
{
  "authenticator": "snowflake_jwt",
  "use_odbc": true,
  "account": "myaccount",
  "username": "myuser",
  "key": "/path/to/private_key.pem",
  "passcode": "optional_passphrase",
  "database": "mydb",
  "schema": "myschema",
  "warehouse": "mywh"
}
```

**OAuth** (ODBC only):
```json
{
  "authenticator": "oauth",
  "account": "myaccount",
  "oauth_client_id": "your_client_id",
  "oauth_client_secret": "your_client_secret",
  "oauth_access_token": "current_access_token",
  "oauth_refresh_token": "refresh_token",
  "database": "mydb",
  "schema": "myschema",
  "warehouse": "mywh"
}
```

**External Browser SSO** (ODBC only):
```json
{
  "authenticator": "externalbrowser",
  "use_odbc": true,
  "account": "myaccount",
  "database": "mydb",
  "schema": "myschema",
  "warehouse": "mywh"
}
```

## Testing

### Basic ODBC Test

Run the included test script to verify ODBC setup:

```bash
docker-compose exec web php /src/dreamfactory/df-snowflake/test_odbc.php
```

This will verify:
- PHP ODBC extension is loaded
- Snowflake ODBC driver is configured
- Connector classes are loadable

### Test with Real Credentials

**Username/Password:**
```bash
docker-compose exec web php /src/dreamfactory/df-snowflake/test_odbc.php \
  --account=myaccount \
  --username=myuser \
  --password=mypass \
  --database=mydb \
  --schema=myschema \
  --warehouse=mywh
```

**OAuth:**
```bash
docker-compose exec web php /src/dreamfactory/df-snowflake/test_odbc.php \
  --account=myaccount \
  --token=<access_token> \
  --username=myuser \
  --database=mydb \
  --schema=myschema \
  --warehouse=mywh
```

## Development Workflow

1. **Docker Environment**:
   ```bash
   cd dreamfactory
   docker-compose build web
   docker-compose up -d
   ```

2. **Link Package**:
   ```bash
   ./link.sh df-snowflake
   ```

3. **Test Changes**:
   ```bash
   docker-compose exec web php /src/dreamfactory/df-snowflake/test_odbc.php
   ```

## API Compatibility

All existing Snowflake API endpoints continue to work identically:
- `GET /api/v2/{service_name}/_table/{table_name}` - List records
- `POST /api/v2/{service_name}/_table/{table_name}` - Create records
- `GET /api/v2/{service_name}/_proc/{procedure_name}` - Call stored procedures
- `GET /api/v2/{service_name}/_func/{function_name}` - Call functions

API documentation generation continues to work as before.

## OAuth Implementation Notes

### Current Status

The basic ODBC infrastructure is in place. To complete OAuth support, you'll need to implement:

1. **OAuth Authorization Flow** - A controller/endpoint to handle:
   - Redirecting users to Snowflake's OAuth consent page
   - Handling the OAuth callback
   - Exchanging authorization code for access/refresh tokens
   - Storing tokens in the service configuration

2. **Token Refresh Logic** - Middleware to:
   - Check token expiration before each request
   - Auto-refresh using refresh token when needed
   - Handle refresh failures gracefully

3. **UI Integration** - Admin interface updates for:
   - "Authorize with Snowflake" button
   - Token status display
   - Manual token refresh option

### OAuth Flow (To Be Implemented)

```
1. User clicks "Authorize with Snowflake" in admin UI
2. Redirect to: https://<account>.snowflakecomputing.com/oauth/authorize
   - Parameters: client_id, redirect_uri, response_type=code, scope
3. User authenticates and grants permission
4. Snowflake redirects back with authorization code
5. Exchange code for tokens:
   POST https://<account>.snowflakecomputing.com/oauth/token-request
6. Store access_token, refresh_token, expires_at in service config
7. Subsequent API calls use access_token via ODBC DSN
```

## Troubleshooting

### ODBC Driver Not Found

```bash
# Check if driver is installed
docker-compose exec web odbcinst -q -d

# Should show: [Snowflake]
```

### PHP ODBC Extension Missing

```bash
# Check if extension is loaded
docker-compose exec web php -m | grep odbc

# Should show: odbc
```

### Connection Errors

Check logs in the container:
```bash
docker-compose logs -f web
```

Enable debug logging in config:
```json
{
  "APP_LOG_LEVEL": "debug",
  "APP_DEBUG": "true"
}
```

## Next Steps

1. **Implement OAuth Handler** - Create service for OAuth flow
2. **Token Refresh Middleware** - Auto-refresh tokens
3. **Admin UI Updates** - Add OAuth authorization interface
4. **Integration Tests** - Test with real Snowflake OAuth app
5. **Documentation** - User guide for OAuth setup
6. **Performance Testing** - Compare PDO vs ODBC performance

## Technical Notes

### ODBC vs PDO

**ODBC Advantages**:
- Native support for OAuth and external browser auth
- Better support for advanced Snowflake features
- Direct driver updates from Snowflake

**PDO Advantages**:
- Laravel standard
- Prepared statement support
- Slightly better PHP integration

**Current Implementation**:
- PDO by default (backward compatibility)
- ODBC auto-enabled for OAuth
- Manual override with `use_odbc` flag

### Query Translation

ODBC doesn't support prepared statements like PDO, so we:
1. Manually bind parameters in `bindParameters()`
2. Escape values using SQL standard quote doubling
3. Maintain query builder compatibility

## Files Modified/Created

### New Files
- `src/Database/Connectors/SnowflakeOdbcConnector.php`
- `src/Database/SnowflakeOdbcConnection.php`
- `test_odbc.php`
- `ODBC-README.md`

### Modified Files
- `src/Models/SnowflakeDbConfig.php` - Added OAuth fields
- `src/ServiceProvider.php` - Added ODBC routing logic
- `dreamfactory/Dockerfile` - Added ODBC driver installation
- `dreamfactory/odbcinst.ini` - ODBC driver config
- `dreamfactory/odbc.ini` - ODBC data sources config

## Branch Information

- **Branch**: `feature/snowflake-odbc-oauth`
- **Base**: `master`
- **Status**: Development/Testing
- **NOT merged to master** ✓

## Resources

- [Snowflake ODBC Driver Documentation](https://docs.snowflake.com/en/developer-guide/odbc/odbc)
- [Snowflake OAuth Documentation](https://docs.snowflake.com/en/user-guide/oauth)
- [Laravel Database Connections](https://laravel.com/docs/database)
- [PHP ODBC Functions](https://www.php.net/manual/en/ref.uodbc.php)
